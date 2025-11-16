from pathlib import Path
from datetime import datetime, timedelta

import typer
import polars as pl

MAXJEUNE_DATA_URL = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/tgvmax/exports/csv"
DATA_FOLDER = Path("data")
SCHEMA = {
    "date": pl.Date,
    # "heure_depart": pl.Time,
    # "heure_arrivee": pl.Time,
    "request_date": pl.Datetime,
}

app = typer.Typer()


def scan_csv():
    """Read and parse all Max Jeune CSV files."""
    return pl.scan_csv(
        DATA_FOLDER / "maxjeune" / "*.csv",
        schema_overrides=SCHEMA,
        include_file_paths="file_path",
    ).with_columns(
        pl.col("heure_depart").str.to_time("%H:%M"),
        pl.col("heure_arrivee").str.to_time("%H:%M"),
        has_seat=pl.col("od_happy_card") == "OUI",
        days_to_trip=(pl.col("date") - pl.col("request_date")).dt.total_days(),
    )


@app.command()
def download_maxjeune():
    """Download the maxjeune data.

    No checks if already downloaded.
    WARNING: used by CI to scrape every day
    """
    maxjeune_folder = DATA_FOLDER / "maxjeune"
    maxjeune_folder.mkdir(exist_ok=True, parents=True)

    file_numbers = sorted(int(file.stem) for file in maxjeune_folder.glob("*.csv"))
    if len(file_numbers) == 0:
        next_file = maxjeune_folder / "1.csv"
    else:
        next_file = maxjeune_folder / f"{int(file_numbers[-1]) + 1}.csv"

    data = pl.read_csv(MAXJEUNE_DATA_URL).with_columns(request_date=datetime.now())
    data.write_csv(next_file)

    print(f"File downloaded to {next_file}")


@app.command()
def has_missing_requests():
    """Find requests that are missing in the downloaded data."""
    requests = (
        scan_csv()
        .select("file_path", "request_date")
        .unique(("file_path", "request_date"))
        .sort("request_date")
        .collect()
    )

    time_diffs = (
        requests.select(diff=pl.col("request_date").dt.date().diff().drop_nulls())
        .filter(pl.col("diff").dt.total_days() > 1)
        .count()
        .item()
    )
    n_requested_days = requests.n_unique(pl.col("request_date").dt.date())
    first_day = requests.select(pl.col("request_date").dt.date().min()).item()
    last_day = requests.select(pl.col("request_date").dt.date().max()).item()

    print(
        f"Total of {n_requested_days} days of data with {time_diffs} request missing between {first_day} and {last_day}"
    )
    print(f"Found {time_diffs} request days missing")
    return time_diffs


if __name__ == "__main__":
    app()
