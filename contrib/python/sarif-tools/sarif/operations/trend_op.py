"""
Code for `sarif trend` command.
"""

import csv
from typing import Dict, List, Literal

from sarif import sarif_file
from sarif.sarif_file import SarifFileSet

TIMESTAMP_COLUMNS = ["Date", "Tool", *sarif_file.SARIF_SEVERITIES_WITH_NONE]


def generate_trend_csv(
    input_files: SarifFileSet,
    output_file: str,
    dateformat: Literal["dmy", "mdy", "ymd"],
) -> None:
    """
    Generate a timeline csv of the issues from the SARIF files.  Each SARIF file must contain a
    timestamp of the form 20211012T110000Z in its filename.
    sarif_dict is a dict from filename to deserialized SARIF data.
    output_file is the name of a CSV file to write, or if None, the name
    `static_analysis_trend.csv` will be used.
    """
    if not output_file:
        output_file = "static_analysis_trend.csv"

    error_storage = []
    for input_file in input_files:
        input_file_name = input_file.get_file_name()
        print("Processing", input_file_name)
        error_list = input_file.get_records()
        tool_name = "/".join(input_file.get_distinct_tool_names())
        # Date parsing
        parsed_date = input_file.get_filename_timestamp()
        if not parsed_date:
            raise ValueError(f"Unable to parse date from filename: {input_file_name}")

        # Turn the date into something that looks nice in excel (d/m/y UK date format)
        dstr = parsed_date[0]
        (year, month, day, hour, minute) = (
            dstr[0:4],
            dstr[4:6],
            dstr[6:8],
            dstr[9:11],
            dstr[11:13],
        )
        if dateformat == "ymd":
            excel_date = f"{year}-{month}-{day} {hour}:{minute}"
        elif dateformat == "mdy":
            excel_date = f"{month}/{day}/{year} {hour}:{minute}"
        else:
            excel_date = f"{day}/{month}/{year} {hour}:{minute}"

        # Store data
        error_storage.append(
            _store_errors(parsed_date, excel_date, tool_name, error_list)
        )

    error_storage.sort(key=lambda record: record["_timestamp"])

    print("Writing trend CSV to", output_file)
    _write_csv(output_file, error_storage)
    filter_stats = input_files.get_filter_stats()
    if filter_stats:
        print(f"  Results are filtered by {filter_stats}")


def _write_csv(output_file: str, error_storage: List[Dict]) -> None:
    with open(output_file, "w", encoding="utf-8") as file_out:
        writer = csv.DictWriter(
            file_out, TIMESTAMP_COLUMNS, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        for key in error_storage:
            writer.writerow(key)


def _store_errors(timestamp, excel_date, tool: str, list_of_errors: List[Dict]) -> Dict:
    results = {
        "_timestamp": timestamp,  # not written to CSV, but used for sorting
        "Date": excel_date,
        "Tool": tool,
    }
    for severity in sarif_file.SARIF_SEVERITIES_WITH_NONE:
        error_count = sum(1 for e in list_of_errors if severity in e["Severity"])
        results[severity] = error_count

    return results
