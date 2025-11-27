from pprint import pprint
import re
from pathlib import Path
from datetime import datetime

import typer
import polars as pl
import altair as alt
from skrub import fuzzy_join, TableReport

MAXJEUNE_DATA_URL = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/tgvmax/exports/csv"

# URL encoding of the following Overpass query
# [out:csv(::lat, ::lon, name, "ref:FR:sncf:resarail", "ref:FR:uic8", "railway:ref"; true; ",")][timeout:150];
# node["ref:FR:sncf:resarail"];
# out;
STATIONS_DATA_URL = "https://overpass-api.de/api/interpreter?data=%5Bout%3Acsv%28%3A%3Alat%2C%20%3A%3Alon%2C%20name%2C%20%22ref%3AFR%3Asncf%3Aresarail%22%2C%20%22ref%3AFR%3Auic8%22%2C%20%22railway%3Aref%22%3B%20true%3B%20%22%2C%22%29%5D%5Btimeout%3A150%5D%3B%0Anode%5B%22ref%3AFR%3Asncf%3Aresarail%22%5D%3B%0Aout%3B"

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

app = typer.Typer()

pl.Config.set_tbl_cols(-1)


def scan_files() -> pl.LazyFrame:
    """Read and parse all Max Jeune CSV files."""

    return pl.scan_parquet(
        DATA_FOLDER / "maxjeune" / "*.pq", include_file_paths="file_path"
    ).with_columns(
        has_seat=pl.col("od_happy_card") == "OUI",
        days_to_trip=(pl.col("date") - pl.col("request_date")).dt.total_days(),
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


def plot_n_trains_availability() -> alt.Chart:
    """Plot the number of available trips in the next 30 days at each request date"""

    # Create the chart
    n_available_trips = (
        scan_files()
        .group_by("request_date")
        .agg(
            disponible=(pl.col("has_seat") == True).sum(),  # noqa: E712
            total=pl.col("has_seat").len(),
        )
        .collect(engine="streaming")
        .unpivot(on=["disponible", "total"], index="request_date")
    )

    alt.renderers.enable("browser")
    plot = (
        alt.Chart(
            n_available_trips,
            width=400,
            height=200,
            title="Historique du nombre de trajets MAXJEUNE et au total, disponibles chaque jour",
        )
        .mark_line()
        .encode(
            x=alt.X(
                "request_date",
                title="Date de la recherche",
                axis=alt.Axis(format="%B %Y"),
            ),
            y=alt.Y("value", title="Nombre de trajets"),
            color=alt.Color("variable").legend(title=None),
        )
        .configure_legend(orient="top")
        .configure_axisX(labelAngle=45)
    )

    return plot


@app.command()
def schema():
    pprint(scan_files().collect_schema())


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
        .group_by(*TRAIN)
        .agg(
            disponible=pl.col("request_date").filter(pl.col("has_seat") == True).sum(),
            total=pl.col("has_seat").len(),
        )
        .filter(
            pl.col("date") > pl.col("date").min() + pl.duration(days=31),
            pl.col("date") < pl.col("date").max() - pl.duration(days=31),
        )
        .group_by("date")
        .agg(pl.col("disponible").mean(), pl.col("total").mean())
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
            title="Historique du nombre de trajets MAXJEUNE et au total, disponibles chaque jour",
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

    for csv_file in from_dir.glob("*.csv"):
        pq_file = to_dir / (csv_file.stem + ".pq")
        pl.read_csv(csv_file, schema_overrides=SCHEMA).with_columns(
            pl.col("heure_depart").str.to_time("%H:%M"),
            pl.col("heure_arrivee").str.to_time("%H:%M"),
        ).drop("_key", "_type").write_parquet(pq_file)
        print(pl.read_parquet(pq_file))


@app.command()
def download_maxjeune():
    """Download the maxjeune data.

    No checks if already downloaded.
    WARNING: used by CI to scrape every day
    """
    maxjeune_folder = DATA_FOLDER / "maxjeune"
    maxjeune_folder.mkdir(exist_ok=True, parents=True)

    file_numbers = sorted(int(file.stem) for file in maxjeune_folder.glob("*.pq"))
    if len(file_numbers) == 0:
        next_file = maxjeune_folder / "1.pq"
    else:
        next_file = maxjeune_folder / f"{int(file_numbers[-1]) + 1}.pq"

    data = pl.read_csv(
        MAXJEUNE_DATA_URL, separator=";", schema_overrides=SCHEMA
    ).with_columns(
        pl.col("heure_depart").str.to_time("%H:%M"),
        pl.col("heure_arrivee").str.to_time("%H:%M"),
        request_date=datetime.now(),
    )
    data.write_parquet(next_file)

    print(f"File downloaded to {next_file}")


@app.command()
def download_aux():
    """Download auxiliary data that will be used in the analysis."""

    stations = pl.read_csv(
        STATIONS_DATA_URL, schema_overrides={"ref:FR:uic8": pl.String}
    )

    stations.write_parquet(DATA_FOLDER / "stations.pq")
    stations.write_csv(DATA_FOLDER / "stations.csv")
    print(f"Downloaded station data to {DATA_FOLDER / "stations.pq"}")

    TableReport(stations).open()


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

    print("Found", len(name_changes), "name changes")
    name_changes.write_json("name_changes.json")

    return name_changes


@app.command()
def iata_inconsistencies():
    stations = pl.read_parquet("data/stations.pq").rename(
        {"ref:FR:sncf:resarail": "iata"}
    )
    print(stations)
    origins = (
        scan_files()
        .filter(pl.col("origine") != "TBD")
        .select("origine", iata="origine_iata")
        .unique("iata")
        .collect(engine="streaming")
    )
    print(origins)
    missing = origins.join(stations, on="iata", how="anti").select("iata")

    print(
        scan_files()
        .filter(pl.col("origine") != "TBD")
        .tail(1_000_000)
        .join(missing.lazy(), left_on="origine_iata", right_on="iata")
        .unique(TRAIN)
        .collect(engine="streaming")
    )

    return

    stations = pl.read_parquet("data/stations.pq").select("iata", "nom")
    origins = (
        scan_files()
        .select(iata=pl.col("origine_iata").unique())
        .collect(engine="streaming")
    )

    destinations = (
        scan_files()
        .select(iata=pl.col("destination_iata").unique())
        .collect(engine="streaming")
    )
    assert (
        len(origins.join(destinations, on="iata", how="anti")) == 0
    ), """The are destination that are not origins (or vice-versa)"""

    print(stations)
    print(origins)

    print(
        "Number of iata in maxjeune data not in stations:",
        len(origins.join(stations, on="iata", how="anti")),
        "/",
        len(origins),
    )
    print(
        "Number of iata in stations data not in maxjeune:",
        len(stations.join(origins, on="iata", how="anti")),
        "/",
        len(stations),
    )

    origins_names = (
        scan_files()
        .filter(pl.col("origine") != "TBD", pl.col("origine") != "")
        .select("origine", "origine_iata")
        .unique(["origine", "origine_iata"])
        .collect(engine="streaming")
    )
    print(origins_names)

    origin_to_station: pl.DataFrame = fuzzy_join(
        origins_names, stations, left_on="origine", right_on="nom", add_match_info=True
    ).filter(pl.col("nom").str.len_chars() > 0)
    print(
        origin_to_station.group_by("origine_iata", "origine")
        .agg(pl.col("iata", "nom").sort_by("skrub_Joiner_rescaled_distance").first())
        .sort("origine_iata")
    )

    station_to_origin: pl.DataFrame = fuzzy_join(
        stations,
        origins_names,
        left_on="nom",
        right_on="origine",
        add_match_info=True,
    ).filter(pl.col("origine").str.len_chars() > 0)
    print(
        station_to_origin.group_by("iata", "nom")
        .agg(
            pl.col("origine_iata", "origine")
            .sort_by("skrub_Joiner_rescaled_distance")
            .first()
        )
        .sort("iata")
    )


@app.command()
def dev():
    stations = pl.read_parquet("data/stations.pq").select(
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
            .filter(file_path="data/maxjeune/432.pq")
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
