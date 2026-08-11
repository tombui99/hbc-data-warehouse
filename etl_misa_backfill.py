"""Manually backfill Misa CRM records into the existing DuckDB warehouse."""

import argparse
import os
from datetime import date, datetime

from etl_misa import run_etl


DEFAULT_START_DATE = "2026-06-01"


def valid_start_date(value):
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("start date must use YYYY-MM-DD format") from exc

    if parsed > date.today():
        raise argparse.ArgumentTypeError("start date cannot be in the future")
    return parsed


def main():
    parser = argparse.ArgumentParser(
        description="Backfill all Misa CRM records modified on or after a date."
    )
    parser.add_argument(
        "--start-date",
        type=valid_start_date,
        default=os.getenv("MISA_BACKFILL_START_DATE", DEFAULT_START_DATE),
        help=f"inclusive lower boundary in YYYY-MM-DD format (default: {DEFAULT_START_DATE})",
    )
    args = parser.parse_args()

    boundary = datetime.combine(args.start_date, datetime.min.time()).isoformat()
    print(f"Starting inclusive Misa CRM backfill from {boundary} through now...")
    run_etl(from_modified=boundary, include_boundary=True)


if __name__ == "__main__":
    main()
