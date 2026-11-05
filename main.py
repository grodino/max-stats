from pprint import pprint
import re
from pathlib import Path
from datetime import datetime

import typer
import polars as pl
import altair as alt
from skrub import fuzzy_join, TableReport

MAXJEUNE_DATA_URL = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/tgvmax/exports/csv"
STATIONS_DATA_URL = "https://raw.githubusercontent.com/trainline-eu/stations/refs/heads/master/stations.csv"
DATA_FOLDER = Path("data")
SCHEMA = {"date": pl.Date, "request_date": pl.Datetime}
TRAIN = [
    "date",
    "train_no",
    "origine_iata",
    "destination_iata",
    "heure_depart",
    "heure_arrivee",
]
IATA_MATCH = {"FRPAZ": "FRESB", "FRPNO": "FRCCP"}


app = typer.Typer()

MAXJEUNE_FOLDER = DATA_FOLDER / "maxjeune"

pl.Config.set_tbl_cols(-1)


def scan_files(apply_trip_filters: bool = False) -> pl.LazyFrame:
    """Read and parse all Max Jeune CSV files. Optionally apply filters."""

    data = pl.scan_parquet(
        MAXJEUNE_FOLDER / "*.parquet", include_file_paths="file_path"
    ).with_columns(
        has_seat=pl.col("od_happy_card") == "OUI",
        days_to_trip=(pl.col("date") - pl.col("request_date")).dt.total_days(),
    )

    if apply_trip_filters:
        data = data.filter(
            *trip_filters(name="origine"), trip_filters(name="destination")
        )

    return data


def dated_file(folder: Path, request_date: datetime) -> Path:
    """Path of the parquet file holding the data requested at `request_date`.

    Files are named after the request date (`YYYY-MM-DD.parquet`). If a file
    already exists for that day (e.g. a delayed scrape that ran after
    midnight), a suffix is added (`YYYY-MM-DD_2.parquet`, ...) so that no data
    is overwritten.
    """
    stem = request_date.strftime("%Y-%m-%d")
    path = folder / f"{stem}.parquet"
    suffix = 2
    while path.exists():
        path = folder / f"{stem}_{suffix}.parquet"
        suffix += 1

    return path


def trip_filters(name="name", axe="axe"):
    """Filters to remove trips that have an inconsistent name or are oustide of
    MAXJEUNE scope

    - Some origins/destinations are described as TBD, then with a name in the
      files. I remove pairs with TBD as name.
    - Some origin/destinations are empty strings.I remove them.
    - Some origins/destinations correspond to buses. I remove them.
    - Some origins/destinations correspond to international train stations. I
      remove them.
    """
    return (
        pl.col(name) != "TBD",
        pl.col(name) != "",
        pl.col(axe) != "AUTOCAR SNCF",
        pl.col(axe) != "INTERNATIONAL",
    )


def has_missing_requests():
    """Find requests that are missing in the downloaded data."""
    requests = (
        scan_files()
        .select("file_path", "request_date")
        .unique(("file_path", "request_date"))
        .sort("request_date")
        .collect(engine="streaming")
    )

    n_missing_days = (
        requests.select(diff=pl.col("request_date").dt.date().diff().drop_nulls())
        .filter(pl.col("diff").dt.total_days() > 1)
        .count()
        .item()
    )
    n_requested_days = requests.n_unique(pl.col("request_date").dt.date())
    first_day = requests.select(pl.col("request_date").dt.date().min()).item()
    last_day = requests.select(pl.col("request_date").dt.date().max()).item()

    return {
        "n_requested_days": n_requested_days,
        "n_missing_days": n_missing_days,
        "requests_start": first_day,
        "requests_end": last_day,
    }


def missing_request_days() -> pl.DataFrame:
    """Days between the first and last request without any downloaded data."""
    requested_days = (
        scan_files()
        .select(request_date=pl.col("request_date").dt.date())
        .unique()
        .collect(engine="streaming")
    )
    all_days = pl.DataFrame(
        {
            "request_date": pl.date_range(
                requested_days["request_date"].min(),
                requested_days["request_date"].max(),
                interval="1d",
                eager=True,
            )
        }
    )

    return all_days.join(requested_days, on="request_date", how="anti").sort(
        "request_date"
    )


def plot_n_trains_availability() -> alt.Chart:
    """Plot the number of available trips in the next 30 days at each request date"""

    # Create the chart
    n_available_trips = (
        scan_files(apply_trip_filters=True)
        .group_by("request_date")
        .agg(
            disponible=(pl.col("has_seat") == True).sum(),  # noqa: E712
            total=pl.col("has_seat").len(),
        )
        .collect(engine="streaming")
        .unpivot(on=["disponible", "total"], index="request_date")
    )

    alt.renderers.enable("browser")
    color = alt.Color(
        "variable",
        scale=alt.Scale(
            domain=["disponible", "total", "jour manquant"],
            range=["#4c78a8", "#f58518", "#e45756"],
        ),
    ).legend(title=None)
    lines = (
        alt.Chart(n_available_trips)
        .mark_line()
        .encode(
            x=alt.X(
                "request_date",
                title="Date de la recherche",
                axis=alt.Axis(format="%B %Y"),
            ),
            y=alt.Y("value", title="Nombre de trajets"),
            color=color,
        )
    )

    # Mark the days without any request as red dots on the x axis
    missing_ticks = (
        alt.Chart(
            missing_request_days().with_columns(
                variable=pl.lit("jour manquant")
            )
        )
        .mark_point(filled=True, size=30, opacity=0.9)
        .encode(
            x=alt.X("request_date"),
            y=alt.value(200),
            color=color,
        )
    )

    plot = (
        alt.layer(
            lines,
            missing_ticks,
            width=400,
            height=200,
            title="Historique du nombre de trajets MAXJEUNE et au total, disponibles chaque jour",
        )
        .configure_legend(orient="top")
        .configure_axisX(labelAngle=45)
    )

    return plot


@app.command()
def describe():
    pprint(scan_files().collect_schema())
    print(
        scan_files()
        .select(pl.col("axe").unique())
        .collect(engine="streaming")
        .to_series()
        .to_list()
    )


@app.command()
def plot_n_days_availability() -> alt.Chart:
    """Plot the number of days a trip was available in the 30 days before its departure.

    NOTE: some trips have multiple carriages, thus might be counted twice in the
    total and available counts.
    """

    TRAIN = [
        "date",
        "train_no",
        "origine_iata",
        "destination_iata",
        "heure_depart",
        "heure_arrivee",
    ]

    n_available_days = (
        scan_files()
        # In case of trains with multiple carriages, we aggregate the disponibility of seats.
        # .group_by(*TRAIN)
        # .agg(
        #     disponible=pl.col("request_date").filter(pl.col("has_seat") == True).sum(),
        #     total=pl.col("has_seat").len(),
        # )
        .filter(
            pl.col("date") > pl.col("date").min() + pl.duration(days=31),
            pl.col("date") < pl.col("date").max() - pl.duration(days=31),
        )
        .group_by("date")
        .agg(
            disponible=pl.col("has_seat").sum(),
            total=pl.col("has_seat").len(),
        )
        # .agg(pl.col("disponible").mean(), pl.col("total").mean())
        .collect(engine="streaming")
        .unpivot(on=["disponible", "total"], index="date")
    )

    alt.renderers.enable("browser")
    alt.data_transformers.enable("vegafusion")
    plot = (
        alt.Chart(
            n_available_days,
            width=400,
            height=200,
            title="Historique du nombre de jours les trajets MAXJEUNE sont disponibles en moyenne",
        )
        .mark_line()
        .encode(
            x=alt.X(
                "date",
                title="Date du train",
                axis=alt.Axis(format="%B %Y"),
            ),
            y=alt.Y("value", title="Nombre de jours"),
            color=alt.Color("variable").legend(title=None),
            tooltip=["date", "value"],
        )
        .configure_legend(orient="top")
        .configure_axisX(labelAngle=45)
    )
    plot.show()

    return plot


@app.command()
def convert(from_dir: Path, to_dir: Path):
    """Convert and clean data scraped with scrapy to parquet."""

    for csv_file in sorted(from_dir.glob("*.csv"), key=lambda f: int(f.stem)):
        file_id = int(csv_file.stem)

        if file_id >= 461:
            data = (
                pl.read_csv(csv_file, schema_overrides=SCHEMA)
                .with_columns(
                    pl.col("heure_depart").str.to_time("%H:%M"),
                    pl.col("heure_arrivee").str.to_time("%H:%M"),
                )
                .drop("_key", "_type")
            )
            pq_file = dated_file(to_dir, data["request_date"].min())
            data.write_parquet(pq_file)
            print(pl.read_parquet(pq_file))


@app.command()
def download_maxjeune():
    """Download the maxjeune data.

    No checks if already downloaded.
    WARNING: used by CI to scrape every day
    """
    MAXJEUNE_FOLDER.mkdir(exist_ok=True, parents=True)

    request_date = datetime.now()
    data = pl.read_csv(
        MAXJEUNE_DATA_URL, separator=";", schema_overrides=SCHEMA
    ).with_columns(
        pl.col("heure_depart").str.to_time("%H:%M"),
        pl.col("heure_arrivee").str.to_time("%H:%M"),
        request_date=request_date,
    )
    next_file = dated_file(MAXJEUNE_FOLDER, request_date)
    data.write_parquet(next_file)

    print(f"File downloaded to {next_file}")


@app.command()
def download_aux():
    """Download auxiliary data that will be used in the analysis."""

    stations = pl.read_csv(
        STATIONS_DATA_URL,
        separator=";",
        schema_overrides={
            "name": pl.String,
            "latitude": pl.Float64,
            "longitude": pl.Float64,
            "sncf_id": pl.String,
        },
        infer_schema=False,
    ).rename({"sncf_id": "iata"})

    stations.write_parquet(DATA_FOLDER / "stations.parquet")
    stations.write_csv(DATA_FOLDER / "stations.csv")
    print(f"Downloaded station data to {DATA_FOLDER / "stations.parquet"}")


@app.command()
def update_readme():
    """Add stats and a chart to README.md.

    Plot the number of available trips at each request date and update the
    readme with the total number of request days and missing days
    """

    plot = plot_n_trains_availability()
    plot.save("assets/n_available_trips.svg")

    # Update the readme
    readme_str = Path("README.md").read_text()

    for variable, value in has_missing_requests().items():
        to_replace = re.search(rf'<span id="{variable}">[^<]*<\/span>', readme_str)[0]

        if isinstance(value, datetime):
            value = value.strftime("%Y/%m/%d")
        else:
            value = str(value)

        # Add the html around it
        value = f'<span id="{variable}">{value}</span>'

        readme_str = readme_str.replace(to_replace, value)

    Path("README.md").write_text(readme_str)


@app.command()
def name_changes():
    """Print iata codes that have multiple names.

    Some origin/destinations changed named for the same IATA identifier
    during the data collection. This detects those.
    """

    origin_name_changes = (
        scan_files()
        .group_by("origine", "origine_iata")
        .agg()
        .group_by(iata="origine_iata")
        .agg(names=pl.col("origine").unique())
        .filter(pl.col("names").list.len() > 1)
        .collect(engine="streaming")
    )

    destination_name_changes = (
        scan_files()
        .group_by("destination", "destination_iata")
        .agg()
        .group_by(iata="destination_iata")
        .agg(names=pl.col("destination").unique())
        .filter(pl.col("names").list.len() > 1)
        .collect(engine="streaming")
    )

    name_changes = (
        destination_name_changes.join(origin_name_changes, on="iata", how="full")
        .with_columns(
            pl.col("names").fill_null(pl.lit([])),
            pl.col("names_right").fill_null(pl.lit([])),
        )
        .with_columns(names=pl.concat_list("names", "names_right").list.unique())
        .drop("names_right", "iata_right")
    )

    for change in name_changes.to_dicts():
        print(change["iata"], ", ".join(change["names"]))

    print("Found", len(name_changes), "name changes")

    return name_changes


@app.command()
def match_iata():
    """Validate that the iata ids from the MAXJEUNE data match the iata ids in
    the trainline train station data"""

    ############################################################################
    # 1. Collect all (name, iata) from origins and destinations.               #
    ############################################################################
    origins = (
        scan_files(apply_trip_filters=True)  # noqa: F821
        .unique(["origine", "origine_iata"])
        .select("axe", name="origine", iata="origine_iata")
        .collect(engine="streaming")
    )
    destinations = (
        scan_files(apply_trip_filters=True)
        .unique(["destination", "destination_iata"])
        .select(
            "axe",
            name="destination",
            iata="destination_iata",
        )
        .collect(engine="streaming")
    )
    pairs: pl.DataFrame = pl.concat([origins, destinations]).unique(["name", "iata"])

    ############################################################################
    # 2. Filter out (name, iata) pairs that are not relevant for MAXJEUNE.     #
    ############################################################################
    pairs = pairs.filter(trip_filters())

    ############################################################################
    # 3. Match iata to the resarail iata.                                      #
    ############################################################################
    stations = pl.read_parquet("data/stations.parquet").select(
        "name", "iata", "latitude", "longitude"
    )

    if len(pairs.join(stations, on="iata", how="anti")) > 0:
        print(pairs.join(stations, on="iata", how="anti"))
        print("Found (origin, iata) pairs with no equivalents in stations.parquet.")
    else:
        with pl.Config(tbl_rows=500):
            print(pairs.join(stations, on="iata", how="left").sort("iata"))
        print(
            "All origins and destinations (after filters) have a corresponding station."
        )


@app.command()
def dev():
    """Some tests wit QGIS"""
    stations = pl.read_parquet("data/stations.parquet").select(
        "iata", "lattitude", "longitude", "nom"
    )
    # available_seats = (
    #     scan_files()
    #     .group_by("request_date", "origine_iata", "destination_iata")
    #     .agg(available=pl.col("has_seat").sum(), total=pl.col("has_seat").len())
    #     .collect(engine="streaming")
    #     .join(stations, left_on="origine_iata", right_on="iata")
    #     .rename({"lattitude": "origine_lattitude", "longitude": "origine_longitude"})
    #     .join(stations, left_on="destination_iata", right_on="iata")
    #     .rename(
    #         {"lattitude": "destination_lattitude", "longitude": "destination_longitude"}
    #     )
    # )

    # FIXME: there are stations in the MAXJEUNE data that are not available in
    # the stations data...
    tgv_origins = stations.join(
        scan_files()
        .select(name=pl.col("origine"), iata=pl.col("origine_iata"))
        .group_by("name", "iata")
        .agg()
        .collect(engine="streaming"),
        on="iata",
        how="right",
    )
    tgv_destinations = stations.join(
        scan_files()
        .select(name=pl.col("destination"), iata=pl.col("destination_iata"))
        .group_by("name", "iata")
        .agg()
        .collect(engine="streaming"),
        on="iata",
        how="right",
    )

    print(
        len(tgv_origins) - len(tgv_origins.drop_nulls()),
        "/",
        len(tgv_origins),
        "origin stations missing from stations.csv",
    )
    print(
        len(tgv_destinations) - len(tgv_destinations.drop_nulls()),
        "/",
        len(tgv_destinations),
        "destination stations missing from stations.csv",
    )

    tgv_origins.filter(pl.col("nom").is_null()).write_csv("missing_origins.csv")
    return
    tgv_stations = pl.concat((tgv_origins, tgv_destinations)).unique("iata")

    print(
        len(tgv_stations) - len(tgv_stations.drop_nulls()),
        "/",
        len(tgv_stations),
        "stations missing from stations.csv",
    )

    tgv_stations = tgv_stations.drop_nulls()
    tgv_stations.write_csv("tgv_stations.csv")

    trips = (
        (
            scan_files()
            .filter(file_path=str(MAXJEUNE_FOLDER / "2025-11-16.parquet"))
            .group_by("origine_iata", "destination_iata")
            .agg(available=pl.col("has_seat").sum(), total=pl.col("has_seat").len())
            .collect(engine="streaming")
        )
        .join(
            tgv_stations.select("iata"),
            left_on="origine_iata",
            right_on="iata",
            how="right",
            coalesce=False,
        )
        .join(
            tgv_stations.select("iata"),
            left_on="destination_iata",
            right_on="iata",
            how="right",
            coalesce=False,
        )
        .drop("iata", "iata_right")
    )
    print(trips)
    trips.write_csv("tgv_trips.csv")


if __name__ == "__main__":
    app()
