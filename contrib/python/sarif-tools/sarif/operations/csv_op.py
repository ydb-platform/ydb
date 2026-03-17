"""
Code for `sarif csv` command.
"""

import csv
import os

from sarif import sarif_file
from sarif.sarif_file import SarifFileSet
from sarif.sarif_file_utils import combine_record_code_and_description


def generate_csv(input_files: SarifFileSet, output: str, output_multiple_files: bool):
    """
    Generate a CSV file containing the list of issues from the SARIF files.
    sarif_dict is a dict from filename to deserialized SARIF data.
    """
    output_file = output
    if output_multiple_files:
        for input_file in input_files:
            output_file_name = input_file.get_file_name_without_extension() + ".csv"
            print(
                "Writing CSV summary of",
                input_file.get_file_name(),
                "to",
                output_file_name,
            )
            _write_to_csv(input_file, os.path.join(output, output_file_name))
            filter_stats = input_file.get_filter_stats()
            if filter_stats:
                print(f"  Results are filtered by {filter_stats}")
        output_file = os.path.join(output, "static_analysis_output.csv")
    source_description = input_files.get_description()
    print(
        "Writing CSV summary for",
        source_description,
        "to",
        os.path.basename(output_file),
    )
    _write_to_csv(input_files, output_file)
    filter_stats = input_files.get_filter_stats()
    if filter_stats:
        print(f"  Results are filtered by {filter_stats}")


def _write_to_csv(file_or_files, output_file):
    """
    Write out the errors to a CSV file so that a human can do further analysis.
    """
    list_of_errors = file_or_files.get_records()
    severities = file_or_files.get_severities()
    with open(output_file, "w", encoding="utf-8") as file_out:
        writer = csv.DictWriter(
            file_out,
            sarif_file.get_record_headings(file_or_files.has_blame_info()),
            lineterminator="\n",
        )
        writer.writeheader()
        for severity in severities:
            errors_of_severity = [
                e for e in list_of_errors if e["Severity"] == severity
            ]
            sorted_errors_by_severity = sorted(
                errors_of_severity, key=combine_record_code_and_description
            )
            writer.writerows(error_dict for error_dict in sorted_errors_by_severity)
