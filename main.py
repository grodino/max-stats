from pprint import pprint
import json
import re
from urllib.request import urlopen
from pathlib import Path
from datetime import date, datetime, time, timedelta

import typer
import polars as pl
import altair as alt

MAXJEUNE_DATA_URL = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/tgvmax/exports/csv"
STATIONS_DATA_URL = "https://raw.githubusercontent.com/trainline-eu/stations/refs/heads/master/stations.csv"
# Simplified outline of metropolitan France (IGN Admin Express, Licence Ouverte)
FRANCE_GEOJSON_URL = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/metropole-version-simplifiee.geojson"
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
HAS_SEAT = pl.col("od_happy_card") == "OUI"
REQUEST_DAY = pl.col("request_date").dt.date()
# Day (Europe/Paris) a snapshot describes. Snapshots are taken at night
# (around 23h-3h Paris time) and shifted by 2h so that a 23h snapshot counts
# for the next day: ``date - BOOKING_DAY`` is the number of days before
# departure at the start of that day.
BOOKING_DAY = (
    pl.col("request_date").dt.replace_time_zone("UTC").dt.convert_time_zone("Europe/Paris")
    + pl.duration(hours=2)
).dt.date()
# Departure and arrival times are "HH:MM" strings in the raw CSV files
PARSE_TIMES = pl.col("heure_depart", "heure_arrivee").str.to_time("%H:%M")


app = typer.Typer()

MAXJEUNE_FOLDER = DATA_FOLDER / "maxjeune"
# Tables computed by the analysis commands (not versioned)
GENERATED_FOLDER = DATA_FOLDER / "generated"

pl.Config.set_tbl_cols(-1)

# Color scheme shared by all the figures: a sequential ramp of a single hue
# (light to dark, for heatmaps and maps), a step of that ramp as the main
# series color, a gray for context series and an accent hue for highlighted
# marks.
THEME = {
    "ramp": ["#eef4fb", "#c6dbf2", "#8fb8e6", "#4f8fd2", "#2266b0", "#0b3a75"],
    "main": "#2266b0",
    "context": "#9aa1ab",
    "accent": "#e8662a",
}
# Neutral colors, shared by all schemes
INK = "#1f2328"  # titles
MUTED = "#57606a"  # labels, notes
GRID = "#e6e8eb"
NO_DATA = "#eceae6"  # land without data on the map
BORDER = "#8c8a85"
FONT = "Helvetica Neue, Helvetica, Arial, Liberation Sans, sans-serif"


@alt.theme.register("max_stats", enable=True)
def altair_theme() -> alt.theme.ThemeConfig:
    """Altair style of all the figures (reads `THEME` when a chart is saved)."""
    return {
        "config": {
            "background": "white",
            "font": FONT,
            "view": {"stroke": None},
            "title": {
                "color": INK,
                "fontSize": 14,
                "fontWeight": 600,
                "anchor": "start",
                "offset": 12,
            },
            "axis": {
                "labelColor": MUTED,
                "labelFontSize": 11,
                "titleColor": MUTED,
                "titleFontSize": 11,
                "titleFontWeight": "normal",
                "gridColor": GRID,
                "domainColor": BORDER,
                "tickColor": BORDER,
            },
            "legend": {
                "orient": "top",
                "labelColor": INK,
                "labelFontSize": 11,
                "titleColor": MUTED,
                "titleFontSize": 11,
                "titleFontWeight": "normal",
            },
            "range": {"category": [THEME["main"], THEME["context"], THEME["accent"]]},
        }
    }


def matplotlib_style() -> dict:
    """Matplotlib rcParams matching `altair_theme`."""
    return {
        "font.family": "sans-serif",
        "font.sans-serif": ["Helvetica Neue", "Helvetica", "Arial", "Liberation Sans", "DejaVu Sans"],
        "text.color": INK,
        "axes.titlecolor": INK,
        "axes.titlesize": 12,
        "axes.titleweight": 600,
        "axes.labelcolor": MUTED,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "xtick.labelcolor": MUTED,
        "ytick.labelcolor": MUTED,
    }


def scan_files(apply_trip_filters: bool = False) -> pl.LazyFrame:
    """Read and parse all Max Jeune CSV files. Optionally apply filters."""

    data = pl.scan_parquet(
        MAXJEUNE_FOLDER / "*.parquet", include_file_paths="file_path"
    ).with_columns(
        has_seat=HAS_SEAT,
        days_to_trip=(pl.col("date") - pl.col("request_date")).dt.total_days(),
    )

    if apply_trip_filters:
        data = data.filter(
            *trip_filters(name="origine"), *trip_filters(name="destination")
        )

    return data


def read_remote_csv(url: str, **kwargs) -> pl.DataFrame:
    """Download a CSV file and read it with ``pl.read_csv(**kwargs)``.

    Since polars 2, ``pl.read_csv(url)`` reads through object-store, which
    requires a Content-Length header that the SNCF export API does not send.
    """
    with urlopen(url) as response:
        return pl.read_csv(response.read(), **kwargs)


def generated_file(name: str) -> Path:
    """Path of a generated table in `GENERATED_FOLDER`, creating the folder."""
    GENERATED_FOLDER.mkdir(parents=True, exist_ok=True)
    return GENERATED_FOLDER / name


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
        ~pl.col(name).is_in(["TBD", ""]),
        ~pl.col(axe).is_in(["AUTOCAR SNCF", "INTERNATIONAL"]),
    )


def requested_days() -> pl.LazyFrame:
    """Days (``request_date`` column) with at least one downloaded file."""
    return scan_files().select(request_date=REQUEST_DAY).unique()


def has_missing_requests():
    """Find requests that are missing in the downloaded data.

    ``n_missing_days`` counts the gaps between consecutive requested days.
    """
    day = pl.col("request_date")
    return (
        requested_days()
        .select(
            n_requested_days=pl.len(),
            n_missing_days=(day.sort().diff().dt.total_days() > 1).sum(),
            requests_start=day.min(),
            requests_end=day.max(),
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )


def missing_request_days() -> pl.DataFrame:
    """Days between the first and last request without any downloaded data."""
    requested = requested_days()
    day = pl.col("request_date")

    return (
        requested.select(
            request_date=pl.date_range(day.min(), day.max(), interval="1d")
        )
        .join(requested, on="request_date", how="anti")
        .sort("request_date")
        .collect(engine="streaming")
    )


def plot_n_trains_availability() -> alt.Chart:
    """Plot the number of available trips in the next 30 days at each request date"""

    # Create the chart
    n_available_trips = (
        scan_files(apply_trip_filters=True)
        .group_by("request_date")
        .agg(disponible=pl.col("has_seat").sum(), total=pl.len())
        .unpivot(on=["disponible", "total"], index="request_date")
        .sort("variable", "request_date")
        .collect(engine="streaming")
    )

    alt.renderers.enable("browser")
    color = alt.Color(
        "variable",
        scale=alt.Scale(
            domain=["disponible", "total", "jour manquant"],
            range=[THEME["main"], THEME["context"], THEME["accent"]],
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
        .configure_axisX(labelAngle=45)
    )

    return plot


@app.command()
def describe():
    pprint(scan_files().collect_schema())
    print(
        scan_files()
        .select(pl.col("axe").unique().sort())
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

    n_available_days = (
        scan_files()
        # In case of trains with multiple carriages, we aggregate the disponibility of seats.
        # .group_by(*TRAIN)
        # .agg(
        #     disponible=pl.col("request_date").filter("has_seat").sum(),
        #     total=pl.len(),
        # )
        .filter(
            pl.col("date") > pl.col("date").min() + pl.duration(days=31),
            pl.col("date") < pl.col("date").max() - pl.duration(days=31),
        )
        .group_by("date")
        .agg(disponible=pl.col("has_seat").sum(), total=pl.len())
        # .agg(pl.col("disponible").mean(), pl.col("total").mean())
        .unpivot(on=["disponible", "total"], index="date")
        .sort("variable", "date")
        .collect(engine="streaming")
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
                pl.scan_csv(csv_file, schema_overrides=SCHEMA)
                .with_columns(PARSE_TIMES)
                .drop("_key", "_type")
                .collect()
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
    data = read_remote_csv(
        MAXJEUNE_DATA_URL, separator=";", schema_overrides=SCHEMA
    ).with_columns(PARSE_TIMES, request_date=pl.lit(request_date))
    next_file = dated_file(MAXJEUNE_FOLDER, request_date)
    data.write_parquet(next_file)

    print(f"File downloaded to {next_file}")


@app.command()
def download_aux():
    """Download auxiliary data that will be used in the analysis."""

    stations = read_remote_csv(
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

    with urlopen(FRANCE_GEOJSON_URL) as response:
        (DATA_FOLDER / "france.geojson").write_bytes(response.read())
    print(f"Downloaded France outline to {DATA_FOLDER / "france.geojson"}")


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
        if isinstance(value, datetime):
            value = value.strftime("%Y/%m/%d")

        readme_str, n_replaced = re.subn(
            rf'<span id="{variable}">[^<]*</span>',
            f'<span id="{variable}">{value}</span>',
            readme_str,
        )
        if n_replaced == 0:
            raise ValueError(f'No <span id="{variable}"> in README.md')

    Path("README.md").write_text(readme_str)


def booking_lead_time_table(
    condition: pl.Expr | None = None,
    split_periods: bool = True,
    where: pl.Expr | None = None,
) -> pl.DataFrame:
    """Probability that a MAXJEUNE seat is available, by days before departure.

    One observation is one train leg (``TRAIN``) seen in one daily snapshot.
    Duplicated rows of the same leg (several carriages) are merged: the leg has
    a seat if any of its rows has one. Buses and international trains are
    dropped, and so are OUIGO trains: the dataset always lists them without a
    MAXJEUNE seat (Nov. 2024 - Oct. 2026) and stopped listing them in mid-October
    2026, so it says nothing about their real availability.

    The lead time is counted in calendar days (Europe/Paris) between the
    snapshot and the departure date. Snapshots are taken at night (around
    23h-3h Paris time), so ``days_before = d`` describes availability at the
    start of the day, d days before departure. Snapshots are shifted by 2h so
    that a 23h snapshot counts for the next day.

    By default, departures are split in two periods because SNCF changed its
    quota releases at the start of 2026 (new release 3 days before departure).
    With ``split_periods=False``, all departures whose whole 30-day booking
    window was observed are pooled in a single "Historique complet" period.

    ``condition`` is an optional polars expression evaluated on the raw rows.
    Its value becomes an extra ``condition`` column of the result, so the
    probabilities are conditional on it: ``pl.col("destination_iata")`` gives
    one curve per destination, ``pl.col("axe") == "SUD EST"`` gives one curve
    for True and one for False. The expression should be constant for a given
    train leg (``TRAIN``), otherwise the leg is split between several values.

    ``where`` is an optional polars expression evaluated on the raw rows to
    keep only some of them, e.g. ``trip_filter("PARIS (intramuros)", "FRBOJ")``
    to restrict the table to one trip.
    """
    if split_periods:
        period = (
            pl.when(pl.col("date") >= pl.date(2026, 3, 1))
            .then(pl.lit("Départs depuis mars 2026"))
            .when(
                pl.col("date").is_between(pl.date(2025, 1, 1), pl.date(2025, 12, 14))
            )
            .then(pl.lit("Départs en 2025"))
        )
    else:
        first_request, last_request = (
            scan_files()
            .select(first=REQUEST_DAY.min(), last=REQUEST_DAY.max())
            .collect(engine="streaming")
            .row(0)
        )
        period = pl.when(
            pl.col("date").is_between(first_request + timedelta(days=31), last_request)
        ).then(pl.lit("Historique complet"))

    booking_day = BOOKING_DAY
    keys = ["period", "days_before"]
    if condition is not None:
        keys.append("condition")

    def snapshot_counts(file: Path) -> pl.DataFrame:
        """Available and total train legs of one daily snapshot, by ``keys``.

        Each file is one snapshot with a single ``request_date``, so
        ``period`` and ``days_before`` are constant for a train leg and can be
        computed before merging its rows. Files are processed one at a time:
        merging the legs of all snapshots in a single query needs one group
        per leg and snapshot, which does not fit in memory.
        """
        return (
            pl.scan_parquet(file)
            .filter(
                *trip_filters(name="origine"),
                *trip_filters(name="destination"),
                ~pl.col("axe").str.starts_with("OUIGO"),
                pl.lit(True) if where is None else where,
            )
            .with_columns(
                period=period,
                days_before=(pl.col("date") - booking_day).dt.total_days(),
                **({} if condition is None else {"condition": condition}),
            )
            .filter(pl.col("period").is_not_null(), pl.col("days_before") >= 0)
            .group_by(*TRAIN, *keys)
            .agg(has_seat=HAS_SEAT.any())
            .group_by(keys)
            .agg(available=pl.col("has_seat").sum(), total=pl.len())
            .collect(engine="streaming")
        )

    def merge(counts: list[pl.DataFrame]) -> pl.DataFrame:
        return pl.concat(counts).group_by(keys).agg(pl.col("available", "total").sum())

    # Sum the snapshots by batches: with a fine-grained condition (e.g. one
    # value per trip), concatenating all of them first does not fit in memory.
    files = sorted(MAXJEUNE_FOLDER.glob("*.parquet"))
    counts = merge(
        [merge([snapshot_counts(f) for f in files[i : i + 50]]) for i in range(0, len(files), 50)]
    )
    return counts.with_columns(probability=pl.col("available") / pl.col("total")).sort(keys)


# Big stations, as hubs: hub name -> station names in the data
HUBS = {
    "Paris": ["PARIS (intramuros)", "PARIS MONTPARNASSE VAUGIRARD"],
    "Lyon": ["LYON (intramuros)", "LYON ST EXUPERY TGV."],
    "Marseille": ["MARSEILLE ST CHARLES", "MARSEILLE BLANCARDE"],
    "Lille": ["LILLE (intramuros)"],
    "Bordeaux": ["BORDEAUX ST JEAN"],
    "Rennes": ["RENNES"],
    "Nantes": ["NANTES"],
    "Toulouse": ["TOULOUSE MATABIAU"],
    "Strasbourg": ["STRASBOURG"],
    "Montpellier": ["MONTPELLIER SAINT ROCH", "MONTPELLIER SUD DE FRANCE"],
}
PERIODS = {"2026": "Départs depuis mars 2026", "2025": "Départs en 2025"}
# MAXJEUNE "périodes de forte affluence", when fewer seats are opened to the
# subscription (CGV): Friday and Sunday afternoons and evenings, the eves of
# big holiday departures and long weekends ("ponts"). SNCF gives no hour:
# afternoon is taken as departures from PEAK_FROM.
PEAK_FROM = time(12, 0)
AFFLUENCE = ["Hors forte affluence", "Forte affluence"]


def easter(year: int) -> date:
    """Easter Sunday (anonymous Gregorian algorithm)."""
    a, b, c = year % 19, year // 100, year % 100
    d, e = b // 4, b % 4
    g = (8 * b + 13) // 25
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 19 * l) // 433
    month = (h + l - 7 * m + 90) // 25
    return date(year, month, (h + l - 7 * m + 33 * month + 19) % 32)


def public_holidays(year: int) -> list[date]:
    """French public holidays (metropolitan France)."""
    sunday = easter(year)
    return [
        date(year, 1, 1),
        sunday + timedelta(days=1),  # lundi de Pâques
        date(year, 5, 1),
        date(year, 5, 8),
        sunday + timedelta(days=39),  # Ascension
        sunday + timedelta(days=50),  # lundi de Pentecôte
        date(year, 7, 14),
        date(year, 8, 15),
        date(year, 11, 1),
        date(year, 11, 11),
        date(year, 12, 25),
    ]


def holiday_peak_days(years: range = range(2024, 2028)) -> list[date]:
    """Days, other than Fridays and Sundays, whose afternoon is a peak
    period: the eve and the last day of each public holiday, extended to the
    adjacent weekend and to the bridge day ("pont": the Friday after a
    Thursday holiday, the Monday before a Tuesday holiday)."""
    days = set()
    for holiday in (h for year in years for h in public_holidays(year)):
        first = last = holiday
        if holiday.isoweekday() == 4:  # Thursday: pont on Friday
            last = holiday + timedelta(days=1)
        if holiday.isoweekday() == 2:  # Tuesday: pont on Monday
            first = holiday - timedelta(days=1)
        while (first - timedelta(days=1)).isoweekday() >= 6:  # weekend before
            first -= timedelta(days=1)
        while (last + timedelta(days=1)).isoweekday() >= 6:  # weekend after
            last += timedelta(days=1)
        days |= {first - timedelta(days=1), last}
    return sorted(d for d in days if d.isoweekday() not in (5, 7))


def affluence() -> pl.Expr:
    """``AFFLUENCE`` label of a train leg, from its departure date and time."""
    peak_day = pl.col("date").dt.weekday().is_in([5, 7]) | pl.col("date").is_in(
        holiday_peak_days()
    )
    return (
        pl.when(peak_day & (pl.col("heure_depart") >= PEAK_FROM))
        .then(pl.lit(AFFLUENCE[1]))
        .otherwise(pl.lit(AFFLUENCE[0]))
    )


def hub_of(name: str) -> pl.Expr:
    """Hub of the ``name`` station column (``"origine"`` or ``"destination"``),
    null for other stations."""
    stations = {station.upper(): hub for hub, names in HUBS.items() for station in names}
    return pl.col(name).str.to_uppercase().replace_strict(stations, default=None)


def hub_lead_time_table(period: str) -> pl.DataFrame:
    """Number of trips with a MAXJEUNE seat by hub, direction, MAXJEUNE
    period (``affluence``: peak or off-peak, from the departure date and
    time) and days before departure: mean and standard deviation over the
    departure dates.

    A trip is a train leg (``TRAIN``) whose origin (direction "Départs") or
    destination ("Arrivées") is a station of the hub; legs within one hub
    (e.g. Lyon Part-Dieu -> Perrache) are left out. For each departure date
    and number of days before departure, the amount is the number of such
    legs that had a seat in that day's snapshot. Days with two snapshots
    are averaged, days without snapshot are left out, and so are departures
    after the last snapshot (whose 30-day window is not complete). A date
    counts for a period only if it has trains in that period: peak means
    are over Fridays, Sundays and holiday eves, and only count the trains
    leaving from ``PEAK_FROM``.
    """
    origin, destination = hub_of("origine"), hub_of("destination")
    table = (
        booking_lead_time_table(
            condition=pl.struct(
                origin_hub=origin,
                destination_hub=destination,
                date=pl.col("date"),
                affluence_type=affluence(),
            ),
            where=(origin.is_not_null() | destination.is_not_null())
            & origin.ne_missing(destination),
        )
        .filter(pl.col("period") == PERIODS[period])
        .unnest("condition")
    )
    # Number of snapshots of each booking day (one row of each file is enough)
    snapshots = (
        pl.concat(
            pl.scan_parquet(file).select(booking_day=BOOKING_DAY).head(1).collect()
            for file in sorted(MAXJEUNE_FOLDER.glob("*.parquet"))
        )
        .group_by("booking_day")
        .agg(snapshots=pl.len())
    )

    by_date = (
        pl.concat(
            [
                table.filter(pl.col(column).is_not_null()).select(
                    hub=column,
                    direction=pl.lit(direction),
                    *["affluence_type", "date", "days_before", "available"],
                )
                for column, direction in (("origin_hub", "Départs"), ("destination_hub", "Arrivées"))
            ]
        )
        .group_by("hub", "direction", "affluence_type", "date", "days_before")
        .agg(pl.col("available").sum())
        .with_columns(booking_day=pl.col("date") - pl.duration(days=pl.col("days_before")))
        .join(snapshots, on="booking_day")
        .filter(pl.col("date") <= snapshots["booking_day"].max())
        .with_columns(amount=pl.col("available") / pl.col("snapshots"))
    )
    return (
        by_date.group_by("hub", "direction", "affluence_type", "days_before")
        .agg(
            mean=pl.col("amount").mean(),
            std=pl.col("amount").std(),
            departure_dates=pl.len(),
        )
        .with_columns(
            lower=(pl.col("mean") - pl.col("std")).clip(lower_bound=0),
            upper=pl.col("mean") + pl.col("std"),
        )
        .sort("hub", "direction", "affluence_type", "days_before")
    )


def hub_explorer_data(period: str = "2026") -> dict:
    """``hub_lead_time_table`` as nested lists for the trip explorer page:
    series[hub][direction][affluence_type] = {mean, lower, upper, std} indexed by
    days before departure (0-30), plus the number of departure dates."""
    table = hub_lead_time_table(period)
    series: dict = {}
    for (hub, direction, affluence_type), group in table.group_by(
        "hub", "direction", "affluence_type"
    ):
        by_day = {row["days_before"]: row for row in group.iter_rows(named=True)}

        def values(column: str) -> list:
            return [
                None if by_day.get(d, {}).get(column) is None else round(by_day[d][column], 2)
                for d in range(31)
            ]

        series.setdefault(hub, {}).setdefault(direction, {})[affluence_type] = {
            "mean": values("mean"),
            "std": values("std"),
            "lower": values("lower"),
            "upper": values("upper"),
            "departure_dates": group["departure_dates"].max(),
        }
    return {
        "period": PERIODS[period],
        "peak_from": f"{PEAK_FROM:%H}h",
        "hubs": [h for h in HUBS if h in series],
        "series": series,
    }


@app.command()
def booking_lead_time(
    period: str = typer.Option(
        "2026", help="Departures since March 2026 ('2026') or in 2025 ('2025')."
    ),
):
    """Number of trips with a MAXJEUNE seat as a function of the number of
    days before departure, for trips leaving and arriving at each hub
    (``HUBS``): mean over the departure dates, with a band of one standard
    deviation (see ``hub_lead_time_table``).

    Writes ``assets/booking_lead_time.svg`` and
    ``data/generated/booking_lead_time_hubs.csv``.
    """
    if period not in PERIODS:
        raise typer.BadParameter(f"--period must be one of {', '.join(PERIODS)}")
    table = hub_lead_time_table(period)
    table.write_csv(generated_file("booking_lead_time_hubs.csv"))

    best = (
        table.sort("mean", "days_before", descending=True)
        .group_by("hub", "direction", "affluence_type", maintain_order=True)
        .first()
        .sort("hub", "direction", "affluence_type")
    )
    for row in best.iter_rows(named=True):
        print(
            f"{row['hub']:<12} {row['direction']:<9} {row['affluence_type']:<20} "
            f"best time = {row['days_before']:>2} days "
            f"before departure ({row['mean']:.1f} ± {row['std']:.1f} trips with a seat, "
            f"{row['departure_dates']} departure dates)"
        )

    labels = {"Départs": "Départs de la ville", "Arrivées": "Arrivées dans la ville"}
    data = table.with_columns(direction=pl.col("direction").replace_strict(labels))
    color = alt.Color(
        "affluence_type:N",
        title="Période MAXJEUNE",
        scale=alt.Scale(domain=AFFLUENCE, range=[THEME["main"], THEME["accent"]]),
    )
    x = alt.X(
        "days_before:Q",
        title="Jours avant le départ",
        scale=alt.Scale(reverse=True, domain=[30, 0]),
    )
    band = alt.Chart().mark_area(opacity=0.2).encode(
        x=x, y=alt.Y("lower:Q"), y2="upper:Q", color=color
    )
    line = alt.Chart().mark_line(strokeWidth=2).encode(
        x=x,
        y=alt.Y("mean:Q", title="Trajets avec une place"),
        color=color,
        tooltip=[
            alt.Tooltip("affluence_type:N", title="Période"),
            alt.Tooltip("days_before:Q", title="Jours avant"),
            alt.Tooltip("mean:Q", title="Moyenne", format=".1f"),
            alt.Tooltip("std:Q", title="Écart-type", format=".1f"),
        ],
    )
    plot = (
        alt.layer(band, line, data=data)
        .properties(width=260, height=120)
        .facet(
            row=alt.Row("hub:N", title=None, sort=list(HUBS), header=alt.Header(labelFontSize=12)),
            column=alt.Column(
                "direction:N", title=None, sort=list(labels.values()), header=alt.Header(labelFontSize=12)
            ),
        )
        .resolve_scale(y="independent")
        .properties(
            title=alt.Title(
                "Trajets avec une place MAXJEUNE selon le jour de réservation",
                subtitle=f"{PERIODS[period]} · forte affluence : vendredi et dimanche à partir de "
                f"{PEAK_FROM:%H}h, veilles et fins de ponts et jours fériés · "
                "moyenne par jour de départ, zone : ± un écart-type · échelles différentes par ville",
            )
        )
    )
    plot.save("assets/booking_lead_time.svg")
    print("Figure saved to assets/booking_lead_time.svg")


def best_booking_time(table: pl.DataFrame, min_observations: int = 0) -> pl.DataFrame:
    """Best time to book for each group of a ``booking_lead_time_table``.

    The best time is the number of days before departure with the highest
    probability of finding a seat (ties go to the earliest booking).
    ``second_best_days_before`` is the day with the second highest
    probability: when the curve is flat, the best day alone is noisy and the
    second best one tells how far apart the good days are. Groups with fewer
    than ``min_observations`` observations, or that never have a seat, are
    dropped.
    """
    keys = [c for c in ("period", "condition") if c in table.columns]
    return (
        table.group_by(keys)
        .agg(
            best_days_before=pl.col("days_before")
            .sort_by("probability", "days_before", descending=True)
            .first(),
            best_probability=pl.col("probability").max(),
            second_best_days_before=pl.col("days_before")
            .sort_by("probability", "days_before", descending=True)
            .get(1, null_on_oob=True),
            observations=pl.col("total").sum(),
        )
        .filter(
            pl.col("observations") >= min_observations,
            pl.col("best_probability") > 0,
        )
        .sort(keys)
    )


def station_filter(name: str, station: str) -> pl.Expr:
    """Rows whose ``name`` (``"origine"`` or ``"destination"``) is ``station``.

    ``station`` is either an iata code (``FRBOJ``) or a station name as
    written in the data (``"PARIS (intramuros)"``, case insensitive). A name
    can cover several iata codes, e.g. all the stations of Paris.
    """
    station = station.strip().upper()
    return (pl.col(f"{name}_iata") == station) | (
        pl.col(name).str.to_uppercase() == station
    )


def trip_filter(origin: str | None = None, destination: str | None = None) -> pl.Expr:
    """Rows of the trip from ``origin`` to ``destination`` (see ``station_filter``).

    Either end can be ``None`` to keep all the origins or destinations.
    """
    filters = [
        station_filter(name, station)
        for name, station in (("origine", origin), ("destination", destination))
        if station is not None
    ]
    return pl.all_horizontal(filters) if filters else pl.lit(True)


def matched_stations(origin: str | None, destination: str | None) -> pl.DataFrame:
    """Station names and iata codes of the trip, raising if it is never served."""
    matched = (
        scan_files()
        .filter(trip_filter(origin, destination))
        .group_by("origine", "origine_iata", "destination", "destination_iata")
        .agg(rows=pl.len())
        .sort("rows", descending=True)
        .collect(engine="streaming")
    )
    if len(matched) == 0:
        raise typer.BadParameter(
            f"No train from {origin or 'anywhere'} to {destination or 'anywhere'}. "
            "Use an iata code (e.g. FRBOJ) or a station name as written in the "
            "data (e.g. 'PARIS (intramuros)')."
        )
    return matched


def station_coordinates() -> pl.DataFrame:
    """Latitude and longitude of each MAXJEUNE iata code (from download-aux)."""
    return (
        pl.scan_parquet(DATA_FOLDER / "stations.parquet")
        .drop_nulls(["iata", "latitude", "longitude"])
        .group_by("iata")
        .agg(
            # Prefer the main station when several stations share a code
            pl.col("latitude", "longitude")
            .sort_by("is_main_station", descending=True, nulls_last=True)
            .first()
        )
        .collect()
    )


@app.command()
def availability_map(
    min_observations: int = 20_000,
    max_distance_km: float = 60.0,
    since_march_2026: bool = False,
):
    """Map of France colored by the average availability of MAXJEUNE seats
    on trains to each destination, over the full history (default) or only
    for departures since March 2026 (``--since-march-2026``).

    The average availability of a destination is the share of all its
    observations (train leg x daily snapshot, every booking day from 30 days
    before departure to the departure day) that had a MAXJEUNE seat.

    Each point of France takes the value of the nearest destination station,
    if one is closer than ``max_distance_km``; farther areas are left blank.
    Requires ``download-aux`` for station coordinates and the France outline.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.colors import LinearSegmentedColormap, Normalize
    from matplotlib.path import Path as MplPath

    table = booking_lead_time_table(
        condition=pl.col("destination_iata"), split_periods=since_march_2026
    )
    period = "Départs depuis mars 2026" if since_march_2026 else "Historique complet"
    if since_march_2026:
        subtitle = "départs depuis mars 2026"
    else:
        requests = has_missing_requests()
        first = requests["requests_start"] + timedelta(days=31)
        subtitle = (
            f"historique complet, départs du {first:%d/%m/%Y} "
            f"au {requests['requests_end']:%d/%m/%Y}"
        )
    best = (
        table.filter(pl.col("period") == period)
        .group_by(destination_iata="condition")
        .agg(available=pl.col("available").sum(), observations=pl.col("total").sum())
        .with_columns(availability=pl.col("available") / pl.col("observations"))
        .filter(pl.col("observations") >= min_observations)
        .with_columns(iata=pl.col("destination_iata").replace(IATA_MATCH))
        .join(station_coordinates(), on="iata", how="left")
    )
    missing = best.filter(pl.col("latitude").is_null())
    if len(missing) > 0:
        print("No coordinates for", ", ".join(missing["destination_iata"].to_list()))
    best = best.drop_nulls("latitude").sort(
        "availability", "destination_iata", descending=[True, False]
    )
    best.write_csv(generated_file("availability_by_destination.csv"))

    # France outline, keeping only polygons inside metropolitan France
    geometry = json.loads((DATA_FOLDER / "france.geojson").read_text())["geometry"]
    polygons = geometry["coordinates"]
    if geometry["type"] == "Polygon":
        polygons = [polygons]
    paths = [MplPath(np.array(polygon[0])) for polygon in polygons]
    best = best.filter(
        pl.col("longitude").is_between(-5.5, 10),
        pl.col("latitude").is_between(41, 51.5),
    )

    # Nearest-station raster (equirectangular distances are fine at this scale)
    lon = np.linspace(-5.3, 9.7, 600)
    lat = np.linspace(41.2, 51.2, 400)
    grid_lon, grid_lat = np.meshgrid(lon, lat)
    points = np.column_stack([grid_lon.ravel(), grid_lat.ravel()])
    inside = np.zeros(len(points), dtype=bool)
    for path in paths:
        inside |= path.contains_points(points)

    km_per_degree = 111.2
    cos_lat = np.cos(np.radians(46.5))
    station_xy = best.select("longitude", "latitude").to_numpy()
    dx = (points[:, None, 0] - station_xy[None, :, 0]) * cos_lat * km_per_degree
    dy = (points[:, None, 1] - station_xy[None, :, 1]) * km_per_degree
    distance = np.hypot(dx, dy)
    nearest = distance.argmin(axis=1)
    values = best["availability"].to_numpy()[nearest].astype(float)
    values[(distance.min(axis=1) > max_distance_km) | ~inside] = np.nan

    # Continuous sequential ramp: darker = more seats available
    cmap = LinearSegmentedColormap.from_list("ramp", THEME["ramp"])
    norm = Normalize(vmin=0, vmax=np.ceil(best["availability"].max() * 10) / 10)

    plt.rcParams.update(matplotlib_style())
    fig, ax = plt.subplots(figsize=(7, 7))
    for path in paths:
        ax.fill(*path.vertices.T, color=NO_DATA, zorder=0)
        ax.plot(*path.vertices.T, color=BORDER, linewidth=0.6, zorder=2)
    image = ax.pcolormesh(
        grid_lon,
        grid_lat,
        values.reshape(grid_lon.shape),
        cmap=cmap,
        norm=norm,
        shading="auto",
        zorder=1,
        rasterized=True,  # keeps the SVG small (one image instead of 240k cells)
    )
    ax.scatter(
        best["longitude"],
        best["latitude"],
        c=best["availability"],
        cmap=cmap,
        norm=norm,
        s=14,
        edgecolors=INK,
        linewidths=0.5,
        zorder=3,
    )
    ax.set_aspect(1 / cos_lat)
    ax.set_axis_off()
    colorbar = fig.colorbar(image, ax=ax, shrink=0.6, format="{x:.0%}")
    colorbar.set_label("Trains avec une place MAXJEUNE disponible (moyenne)")
    colorbar.outline.set_visible(False)
    colorbar.ax.tick_params(length=0)
    ax.set_title(
        "Disponibilité moyenne des places MAXJEUNE, par destination\n"
        f"({subtitle})",
        loc="left",
    )
    ax.text(
        0,
        -0.02,
        f"En gris : aucune gare de destination à moins de {max_distance_km:.0f} km\n"
        f"Destinations avec au moins {min_observations:,} observations".replace(",", " "),
        transform=ax.transAxes,
        fontsize=8,
        color=MUTED,
        verticalalignment="top",
    )
    fig.savefig("assets/availability_map.svg", bbox_inches="tight", dpi=150)

    names = (
        pl.scan_parquet(DATA_FOLDER / "stations.parquet")
        .group_by("iata")
        .agg(pl.col("name").first())
        .collect()
    )
    ranked = best.join(names, on="iata", how="left", maintain_order="left")
    print(best["availability"].describe())
    print(ranked.select("name", "availability", "observations").head(8))
    print(ranked.select("name", "availability", "observations").tail(8))


@app.command()
def best_time_evolution(
    origin: str | None = typer.Option(
        None, help="Origin station: iata code (FRPMO) or name ('PARIS (intramuros)')."
    ),
    destination: str | None = typer.Option(
        None, help="Destination station: iata code (FRBOJ) or name ('BORDEAUX ST JEAN')."
    ),
    min_observations: int = typer.Option(
        0, help="Drop months with fewer observations (train leg x snapshot)."
    ),
):
    """Plot the evolution of the best day to book, per month of departure.

    Uses the full history (departures whose 30-day booking window was fully
    observed). The heatmap shows, for each month, the probability of finding a
    seat at each booking day relative to the month's best day, which removes
    seasonal variations of the overall level.

    By default all trips are pooled. With ``--origin`` and/or
    ``--destination``, only the trains of that trip are used, and the figure
    and table are written to ``data/generated`` instead of ``assets``.
    """
    title = "Évolution du meilleur moment pour réserver une place MAXJEUNE"
    figure = Path("assets/best_time_evolution.svg")
    best_file = "best_time_by_month.csv"
    where = None
    if origin is not None or destination is not None:
        stations = matched_stations(origin, destination)
        print("Trip stations:")
        print(stations)

        def label(name: str, station: str | None) -> str:
            if station is None:
                return "toutes gares"
            return " / ".join(stations[name].unique(maintain_order=True).to_list())

        trip = f"{label('origine', origin)} → {label('destination', destination)}"
        title = [title, trip]
        slug = re.sub(
            r"[^a-z0-9]+", "-", f"{origin or 'all'}_{destination or 'all'}".lower()
        ).strip("-")
        figure = generated_file(f"best_time_evolution_{slug}.svg")
        best_file = f"best_time_by_month_{slug}.csv"
        where = trip_filter(origin, destination)

    table = booking_lead_time_table(
        condition=pl.col("date").dt.truncate("1mo"), split_periods=False, where=where
    )
    best = best_booking_time(table, min_observations).rename({"condition": "month"})
    if len(best) == 0:
        print("No month with a MAXJEUNE seat for this trip.")
        raise typer.Exit(1)
    table = table.rename({"condition": "month"})
    best.drop("period").write_csv(generated_file(best_file))

    relative = table.join(
        best.select("month", "best_probability"), on="month", maintain_order="left"
    ).with_columns(relative=pl.col("probability") / pl.col("best_probability"))
    markers = best.select(
        "month",
        pl.col("best_days_before").alias("Meilleur jour"),
        pl.col("second_best_days_before").alias("Deuxième meilleur jour"),
    ).unpivot(index="month", variable_name="marker", value_name="days_before")

    x = alt.X("yearmonth(month):O", title="Mois de départ", axis=alt.Axis(format="%b %y", labelAngle=-45))
    y = alt.Y(
        "days_before:O",
        title="Jours avant le départ",
        sort="descending",
        axis=alt.Axis(values=list(range(0, 31, 5))),
    )
    heatmap = (
        alt.Chart(relative)
        .mark_rect()
        .encode(
            x=x,
            y=y,
            color=alt.Color(
                "relative:Q",
                title="Probabilité / meilleur jour du mois",
                # Half of the cells are below 50% of the best day: spend the
                # color steps on the region near the optimum instead
                scale=alt.Scale(
                    type="threshold",
                    domain=[0.5, 0.7, 0.8, 0.9, 0.95],
                    range=THEME["ramp"],
                ),
                legend=alt.Legend(format=".0%", orient="right"),
            ),
            tooltip=[
                alt.Tooltip("yearmonth(month):T", title="Mois"),
                alt.Tooltip("days_before:O", title="Jours avant"),
                alt.Tooltip("probability:Q", title="Probabilité", format=".1%"),
                alt.Tooltip("relative:Q", title="/ meilleur jour", format=".0%"),
            ],
        )
    )
    shape = alt.Shape(
        "marker:N",
        title=None,
        scale=alt.Scale(
            domain=["Meilleur jour", "Deuxième meilleur jour"], range=["circle", "square"]
        ),
        legend=alt.Legend(orient="top", symbolStrokeColor=THEME["accent"]),
    )
    best_points = (
        alt.Chart(markers)
        .transform_filter(alt.datum.marker == "Meilleur jour")
        .mark_point(filled=True, size=40, color=THEME["accent"], stroke="white", strokeWidth=1)
        .encode(x=x, y=y, shape=shape)
    )
    second_best_points = (
        alt.Chart(markers)
        .transform_filter(alt.datum.marker != "Meilleur jour")
        .mark_point(filled=False, size=110, color=THEME["accent"], strokeWidth=2)
        .encode(x=x, y=y, shape=shape)
    )
    plot = (
        alt.layer(heatmap, second_best_points, best_points)
        .resolve_scale(color="independent")
        .properties(
            width=520,
            height=300,
            title=title,
        )
    )
    plot.save(figure)
    print(f"Figure saved to {figure}")

    with pl.Config(tbl_rows=40):
        print(
            best.select(
                "month",
                "best_days_before",
                "second_best_days_before",
                "best_probability",
                "observations",
            )
        )


def trip_explorer_data(n_trips: int) -> dict:
    """Monthly best time to book of the ``n_trips`` trips with the most seats.

    Trips are origin and destination names as written in the data, ranked by
    the number of observations (train leg x daily snapshot, full history) that
    had a MAXJEUNE seat. A trip's numbers are the same as those of
    ``best-time-evolution --origin <origin> --destination <destination>``,
    but all trips are computed in a single pass (filtering each trip with
    ``trip_filter`` takes about 2s per snapshot for 100 trips).
    """
    table = booking_lead_time_table(
        condition=pl.struct(
            origin=pl.col("origine"),
            destination=pl.col("destination"),
            month=pl.col("date").dt.truncate("1mo"),
        ),
        split_periods=False,
    ).unnest("condition")
    ranking = (
        # Legs between two stations of the same city (Lyon Part-Dieu ->
        # Perrache, both "LYON (intramuros)") are not trips
        table.filter(pl.col("origin") != pl.col("destination"))
        .group_by("origin", "destination")
        .agg(pl.col("available", "total").sum())
        .sort(["available", "total", "origin"], descending=[True, True, False])
        .head(n_trips)
    )
    trips = ["origin", "destination"]
    table = table.join(ranking.select(trips), on=trips, how="semi").with_columns(
        condition=pl.struct(*trips, "month")
    )
    best = best_booking_time(table.drop(*trips, "month")).unnest("condition")
    months = sorted(table["month"].unique().to_list())
    month_index = {month: i for i, month in enumerate(months)}

    result = []
    for rank, row in enumerate(ranking.iter_rows(named=True), start=1):
        in_trip = (pl.col("origin") == row["origin"]) & (
            pl.col("destination") == row["destination"]
        )
        # probability[month][days_before], null when the cell was not observed
        probability = [[None] * 31 for _ in months]
        for cell in table.filter(in_trip, pl.col("days_before") <= 30).iter_rows(named=True):
            probability[month_index[cell["month"]]][cell["days_before"]] = round(
                cell["probability"], 4
            )
        result.append(
            {
                "origin": row["origin"],
                "destination": row["destination"],
                "rank": rank,
                "available": row["available"],
                "total": row["total"],
                "probability": probability,
                "best": [
                    {
                        "month": month_index[b["month"]],
                        "best": b["best_days_before"],
                        "second": b["second_best_days_before"],
                        "probability": round(b["best_probability"], 4),
                        "observations": b["observations"],
                    }
                    for b in best.filter(in_trip).sort("month").iter_rows(named=True)
                ],
            }
        )

    requests = has_missing_requests()
    return {
        "generated": f"{requests['requests_end']:%Y-%m-%d}",
        "departures_from": f"{requests['requests_start'] + timedelta(days=31):%Y-%m-%d}",
        "months": [f"{month:%Y-%m}" for month in months],
        "trips": result,
    }


@app.command()
def trip_explorer(
    n_trips: int = typer.Option(100, help="Number of trips to include."),
    k: int = typer.Option(6, help="Number of trip groups (see cluster-trips --choose-k)."),
    seed: int = typer.Option(0, help="Random seed of k-means."),
    output: Path = typer.Option(
        GENERATED_FOLDER / "trip_explorer.html", help="HTML page to write."
    ),
):
    """Interactive page to pick a trip and see the evolution of its best day
    to book (same heatmap as ``best-time-evolution --origin --destination``),
    and to browse the groups of trips found by ``cluster-trips``.

    Trips are listed by number of available MAXJEUNE seats. The page is
    ``templates/trip_explorer.html`` with the data embedded.
    """
    data = trip_explorer_data(n_trips)
    clusters = cluster_trip_data(data, k=k, seed=seed)
    for trip, label, distance in zip(data["trips"], clusters["labels"], clusters["distance"]):
        trip["cluster"] = label
        trip["cluster_distance"] = distance
    data["hubs"] = hub_explorer_data("2026")
    data["clusters"] = {
        "k": k,
        "silhouette": clusters["silhouette"],
        "barycenters": clusters["barycenters"],
    }
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    page = Path("templates/trip_explorer.html").read_text().replace(
        "__TRIP_DATA__", payload.replace("</", "<\\/")
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(page)
    print(
        f"{len(data['trips'])} trips in {k} groups (silhouette "
        f"{clusters['silhouette']:.2f}), page written to {output} ({len(page) / 1e6:.1f} MB)"
    )


def wasserstein_embedding(probability: list[list[float | None]], levels: int):
    """Embed a trip's (month x days before) matrix so that Euclidean distances
    are Wasserstein-2 distances.

    Each month's row is turned into a distribution over the booking days
    (0-30): the share of the month's seats seen at each number of days before
    departure. On a line, the Wasserstein-2 distance between two distributions
    is the L2 distance between their quantile functions, so each month is
    represented by its quantile function sampled at ``levels`` evenly spaced
    levels. The squared Euclidean distance between two embeddings is then the
    mean, over months, of the squared Wasserstein-2 distance (in days), and
    the mean of several embeddings is their Wasserstein barycenter: plain
    k-means clusters the trips with this distance.

    Months without any seat have no distribution: they take the trip's
    average distribution over its other months. Returns the embedding and
    the number of filled months.
    """
    import numpy as np

    seats = np.array(probability, dtype=float)  # None -> nan
    seats = np.nan_to_num(seats, nan=0.0)
    mass = seats.sum(axis=1)
    observed = mass > 0
    distribution = np.empty_like(seats)
    distribution[observed] = seats[observed] / mass[observed, None]
    distribution[~observed] = distribution[observed].mean(axis=0)

    cdf = np.cumsum(distribution, axis=1)
    cdf[:, -1] = 1.0  # rounding errors would push the last quantiles past 30
    u = (np.arange(levels) + 0.5) / levels
    quantiles = np.stack([np.searchsorted(row, u) for row in cdf]).astype(float)
    return quantiles.ravel() / np.sqrt(quantiles.size), int((~observed).sum())


def quantiles_to_distribution(quantiles, n_days: int = 31):
    """Distribution over the booking days of evenly spaced quantiles.

    Barycenter quantiles fall between days: each one is split between the two
    nearest days (linear interpolation), which avoids the stripes of a plain
    histogram.
    """
    import numpy as np

    q = np.clip(quantiles, 0, n_days - 1)
    low = np.floor(q).astype(int)
    frac = q - low
    high = np.minimum(low + 1, n_days - 1)
    share = np.zeros(n_days)
    np.add.at(share, low, 1 - frac)
    np.add.at(share, high, frac)
    return share / len(q)


def choose_k(
    X, n_months: int, max_k: int = 12, n_boot: int = 30, n_ref: int = 20, seed: int = 0
) -> pl.DataFrame:
    """Scores to choose the number of k-means clusters of the embeddings ``X``
    (one row per trip, ``n_months`` blocks of quantiles, see
    ``wasserstein_embedding``), for k = 1..``max_k``.

    - ``rms_days``: RMS distance of the trips to their centroid, in days.
    - ``silhouette``: mean silhouette (k >= 2), higher is better separated.
    - ``gap`` and ``gap_sd``: gap statistic (Tibshirani, Walther and Hastie,
      2001), log within-cluster dispersion of ``n_ref`` reference datasets
      without clusters (uniform in the box of the principal components of
      ``X``) minus that of ``X``. The usual rule takes the smallest k with
      gap(k) >= gap(k+1) - gap_sd(k+1); k = 1 means no cluster structure.
    - ``stability`` and ``stability_p10``: adjusted Rand index between the
      clusters of all the data and those of ``n_boot`` bootstrap samples of
      the months (k-means refitted with another seed), mean and 10th
      percentile. 1 means the same groups whatever the months used.
    """
    import numpy as np
    from sklearn.cluster import KMeans
    from sklearn.metrics import adjusted_rand_score, silhouette_score

    rng = np.random.default_rng(seed)
    n = len(X)

    def fit(Y, k: int, random_state: int, n_init: int = 10) -> KMeans:
        return KMeans(n_clusters=k, n_init=n_init, random_state=random_state).fit(Y)

    def log_dispersion(Y, k: int) -> float:
        if k == 1:
            return float(np.log(((Y - Y.mean(axis=0)) ** 2).sum()))
        return float(np.log(fit(Y, k, seed).inertia_))

    mean = X.mean(axis=0)
    _, _, vt = np.linalg.svd(X - mean, full_matrices=False)
    scores = (X - mean) @ vt.T
    references = [
        rng.uniform(scores.min(axis=0), scores.max(axis=0), size=scores.shape) @ vt + mean
        for _ in range(n_ref)
    ]
    blocks = X.reshape(n, n_months, -1)

    rows = []
    for k in range(1, max_k + 1):
        reference = np.array([log_dispersion(R, k) for R in references])
        row = {
            "k": k,
            "gap": reference.mean() - log_dispersion(X, k),
            "gap_sd": reference.std() * np.sqrt(1 + 1 / n_ref),
            "rms_days": None,
            "silhouette": None,
            "stability": None,
            "stability_p10": None,
        }
        if k == 1:
            row["rms_days"] = float(np.sqrt(((X - mean) ** 2).sum() / n))
        else:
            model = fit(X, k, seed, n_init=20)
            ari = []
            for b in range(n_boot):
                months = rng.integers(0, n_months, n_months)
                Xb = blocks[:, months].reshape(n, -1)
                ari.append(adjusted_rand_score(model.labels_, fit(Xb, k, seed + 1 + b).labels_))
            row |= {
                "rms_days": float(np.sqrt(model.inertia_ / n)),
                "silhouette": float(silhouette_score(X, model.labels_)),
                "stability": float(np.mean(ari)),
                "stability_p10": float(np.quantile(ari, 0.1)),
            }
        rows.append(row)
    return pl.DataFrame(rows)


def trip_embeddings(data: dict, levels: int = 100):
    """Wasserstein embeddings of all the trips of ``trip_explorer_data`` and
    the number of trip-months without seats that were filled."""
    import numpy as np

    embedded = [wasserstein_embedding(t["probability"], levels) for t in data["trips"]]
    return np.stack([e for e, _ in embedded]), sum(n for _, n in embedded)


def cluster_trip_data(data: dict, k: int, levels: int = 100, seed: int = 0) -> dict:
    """k-means of the trips of ``trip_explorer_data`` with the Wasserstein-2
    distance between their (month x days before) matrices.

    Groups are numbered by size, largest first. Returns, by trip, the group
    (``labels``) and the distance to its barycenter in days (``distance``);
    by group, the barycenter as a (month x days before) matrix of the share
    of the month's seats (``barycenters``); the silhouette of the clustering,
    and the k-means model and embeddings for further analysis.
    """
    import numpy as np
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score

    n_months = len(data["months"])
    X, filled_months = trip_embeddings(data, levels)
    model = KMeans(n_clusters=k, n_init=20, random_state=seed).fit(X)

    order = np.argsort(-np.bincount(model.labels_, minlength=k), kind="stable")
    relabel = np.empty(k, dtype=int)
    relabel[order] = np.arange(k)
    labels = relabel[model.labels_]
    centers = model.cluster_centers_[order]
    distance = np.linalg.norm(X - centers[labels], axis=1)
    quantiles = centers.reshape(k, n_months, levels) * np.sqrt(n_months * levels)

    return {
        "labels": labels.tolist(),
        "distance": distance.round(2).tolist(),
        "barycenters": [
            [quantiles_to_distribution(q).round(4).tolist() for q in quantiles[c]]
            for c in range(k)
        ],
        "median_day": np.median(quantiles, axis=2),  # (group, month)
        "silhouette": float(silhouette_score(X, model.labels_)),
        "filled_months": filled_months,
        "model": model,
        "X": X,
    }


@app.command()
def cluster_trips(
    k: int = typer.Option(
        6, help="Number of clusters (6: most stable in --choose-k, Nov. 2026)."
    ),
    n_trips: int = typer.Option(100, help="Number of trips (with the most seats) to cluster."),
    levels: int = typer.Option(100, help="Quantile levels per month in the embedding."),
    seed: int = typer.Option(0, help="Random seed of k-means."),
    choose: bool = typer.Option(
        False,
        "--choose-k",
        help="Only print the scores to choose k (gap statistic, silhouette, "
        "bootstrap stability) for k = 1..--max-k, without clustering.",
    ),
    max_k: int = typer.Option(12, help="Largest k tried by --choose-k."),
):
    """Cluster trips by their full (days before departure x month) matrix,
    with k-means on the Wasserstein-2 distance between booking-day
    distributions (see ``wasserstein_embedding``).

    Two trips are close when, month by month, their seats appear at the same
    number of days before departure, whatever their overall availability.
    Writes ``trip_clusters.csv`` and ``trip_clusters.svg`` (barycenter of
    each cluster) to ``data/generated``. With ``--choose-k``, prints the
    scores of ``choose_k`` instead and writes them to ``choose_k.csv``.
    """
    data = trip_explorer_data(n_trips)
    trips, months = data["trips"], data["months"]

    if choose:
        X, filled = trip_embeddings(data, levels)
        print(
            f"{len(trips)} trips x {len(months)} months, {filled} "
            "trip-months without seats filled with the trip's average"
        )
        scores = choose_k(X, len(months), max_k=max_k, seed=seed)
        scores.write_csv(generated_file("choose_k.csv"))
        with pl.Config(tbl_rows=-1, float_precision=3):
            print(scores)

        gap, sd = scores["gap"].to_list(), scores["gap_sd"].to_list()
        gap_k = next(
            (i + 1 for i in range(len(gap) - 1) if gap[i] >= gap[i + 1] - sd[i + 1]),
            None,
        )
        tried = scores.filter(pl.col("k") >= 2)
        print(
            "Gap statistic (smallest k with gap(k) >= gap(k+1) - sd(k+1)): "
            + (f"k = {gap_k}" if gap_k else f"no k up to {max_k}")
        )
        print(f"Best silhouette: k = {tried.sort('silhouette')['k'][-1]}")
        print(f"Most stable: k = {tried.sort('stability')['k'][-1]}")
        return

    result = cluster_trip_data(data, k=k, levels=levels, seed=seed)
    print(
        f"{len(trips)} trips x {len(months)} months, {result['filled_months']} "
        f"trip-months without seats filled with the trip's average, "
        f"silhouette {result['silhouette']:.3f}"
    )

    clusters = pl.DataFrame(
        {
            "origin": [t["origin"] for t in trips],
            "destination": [t["destination"] for t in trips],
            "rank": [t["rank"] for t in trips],
            "available": [t["available"] for t in trips],
            "group": [label + 1 for label in result["labels"]],
            "distance_days": result["distance"],
        }
    ).sort("group", "distance_days")
    clusters.write_csv(generated_file("trip_clusters.csv"))

    barycenters = pl.DataFrame(
        [
            {"group": c + 1, "month": f"{month}-01", "days_before": d, "share": share}
            for c, matrix in enumerate(result["barycenters"])
            for month, row in zip(months, matrix)
            for d, share in enumerate(row)
        ]
    ).with_columns(pl.col("month").str.to_date())

    with pl.Config(tbl_rows=-1, fmt_str_lengths=40):
        for c in range(k):
            members = clusters.filter(pl.col("group") == c + 1)
            recent = result["median_day"][c, -6:]
            print(
                f"\nGroup {c + 1}: {len(members)} trips, median booking day of the seats "
                f"over the last 6 months: {', '.join(f'{d:.0f}' for d in recent)}"
            )
            print(members.select("rank", "origin", "destination", "distance_days").head(8))

    plot = (
        alt.Chart(barycenters, title="Barycentre de chaque groupe de trajets (Wasserstein)")
        .mark_rect()
        .encode(
            x=alt.X(
                "yearmonth(month):O",
                title="Mois de départ",
                axis=alt.Axis(format="%b %y", labelAngle=-45),
            ),
            y=alt.Y(
                "days_before:O",
                title="Jours avant le départ",
                sort="descending",
                axis=alt.Axis(values=list(range(0, 31, 5))),
            ),
            color=alt.Color(
                "share:Q",
                title="Part des places du mois",
                scale=alt.Scale(range=THEME["ramp"]),
                legend=alt.Legend(format=".0%", orient="right"),
            ),
            facet=alt.Facet(
                "group:N",
                title=None,
                columns=2,
                header=alt.Header(labelExpr="'Groupe ' + datum.value", labelFontSize=12),
            ),
        )
        .properties(width=320, height=200)
    )
    plot.save(generated_file("trip_clusters.svg"))
    print(f"\nClusters written to {generated_file('trip_clusters.csv')} and trip_clusters.svg")


@app.command()
def name_changes():
    """Print iata codes that have multiple names.

    Some origin/destinations changed named for the same IATA identifier
    during the data collection. This detects those.
    """

    def changes(name: str) -> pl.LazyFrame:
        return (
            scan_files()
            .group_by(iata=f"{name}_iata")
            .agg(names=pl.col(name).unique())
            .filter(pl.col("names").list.len() > 1)
        )

    name_changes = (
        changes("destination")
        .join(changes("origine"), on="iata", how="full")
        .select(
            "iata",
            names=pl.concat_list(
                pl.col("names", "names_right").fill_null([])
            ).list.unique().list.sort(),
        )
        .sort("iata", "names", nulls_last=True)
        .collect(engine="streaming")
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
    trips = scan_files(apply_trip_filters=True)
    pairs = pl.concat(
        [
            trips.select("axe", name=pl.col(name), iata=pl.col(f"{name}_iata"))
            for name in ("origine", "destination")
        ]
    ).unique(["name", "iata"])

    ############################################################################
    # 2. Filter out (name, iata) pairs that are not relevant for MAXJEUNE.     #
    ############################################################################
    pairs = pairs.filter(*trip_filters()).collect(engine="streaming")

    ############################################################################
    # 3. Match iata to the resarail iata.                                      #
    ############################################################################
    stations = pl.read_parquet(DATA_FOLDER / "stations.parquet").select(
        "name", "iata", "latitude", "longitude"
    )

    unmatched = pairs.join(stations, on="iata", how="anti")
    if len(unmatched) > 0:
        print(unmatched)
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
    stations = pl.read_parquet(DATA_FOLDER / "stations.parquet").select(
        "iata", "lattitude", "longitude", "nom"
    )
    # available_seats = (
    #     scan_files()
    #     .group_by("request_date", "origine_iata", "destination_iata")
    #     .agg(available=pl.col("has_seat").sum(), total=pl.len())
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
    def tgv_stations_of(name: str) -> pl.DataFrame:
        return stations.join(
            scan_files()
            .select(name=pl.col(name), iata=pl.col(f"{name}_iata"))
            .unique()
            .collect(engine="streaming"),
            on="iata",
            how="right",
        )

    tgv_origins = tgv_stations_of("origine")
    tgv_destinations = tgv_stations_of("destination")

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
            pl.scan_parquet(MAXJEUNE_FOLDER / "2025-11-16.parquet")
            .group_by("origine_iata", "destination_iata")
            .agg(available=HAS_SEAT.sum(), total=pl.len())
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
