import re
from pathlib import Path
from datetime import datetime

import typer
import polars as pl
import altair as alt

MAXJEUNE_DATA_URL = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/tgvmax/exports/csv"
DATA_FOLDER = Path("data")
SCHEMA = {"date": pl.Date, "request_date": pl.Datetime}

app = typer.Typer()


def scan_files():
    """Read and parse all Max Jeune CSV files."""
    return pl.scan_parquet(
        DATA_FOLDER / "maxjeune" / "*.pq", include_file_paths="file_path"
    ).with_columns(
        has_seat=pl.col("od_happy_card") == "OUI",
        days_to_trip=(pl.col("date") - pl.col("request_date")).dt.total_days(),
    )


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
def has_missing_requests():
    """Find requests that are missing in the downloaded data."""
    requests = (
        scan_files()
        .select("file_path", "request_date")
        .unique(("file_path", "request_date"))
        .sort("request_date")
        .collect()
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

    print(
        f"Total of {n_requested_days} days of data with {n_missing_days} request missing between {first_day} and {last_day}"
    )
    print(f"Found {n_missing_days} request days missing")

    return {
        "n_requested_days": n_requested_days,
        "n_missing_days": n_missing_days,
        "requests_start": first_day,
        "requests_end": last_day,
    }


@app.command()
def update_readme():
    """Plot the number of available trips at each request date and update the
    readme with the total number of request days and missing days"""

    n_available_trips = (
        scan_files()
        .group_by("request_date")
        .agg(
            disponible=(pl.col("has_seat") == True).sum(),
            total=pl.col("has_seat").len(),
        )
        .collect()
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
    plot.save("assets/n_available_trips.svg")

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


if __name__ == "__main__":
    app()
