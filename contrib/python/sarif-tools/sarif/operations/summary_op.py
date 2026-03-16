"""
Code for `sarif summary` command.
"""

import os
from typing import List

from sarif.sarif_file import SarifFileSet


def generate_summary(
    input_files: SarifFileSet, output: str, output_multiple_files: bool
):
    """
    Generate a summary of the issues from the SARIF files.
    sarif_dict is a dict from filename to deserialized SARIF data.
    output_file is the name of a text file to write, or if None, the summary is written to the
    console.
    """
    output_file = output
    if output_multiple_files:
        for input_file in input_files:
            output_file_name = (
                input_file.get_file_name_without_extension() + "_summary.txt"
            )
            output_file = os.path.join(output, output_file_name)
            summary_lines = _generate_summary(input_file)
            print(
                "Writing summary of",
                input_file.get_file_name(),
                "to",
                output_file_name,
            )
            with open(output_file, "w", encoding="utf-8") as file_out:
                file_out.writelines(line + "\n" for line in summary_lines)
        output_file_name = "static_analysis_summary.txt"
        output_file = os.path.join(output, output_file_name)

    summary_lines = _generate_summary(input_files)
    if output:
        print(
            "Writing summary of",
            input_files.get_description(),
            "to",
            output_file,
        )
        with open(output_file, "w", encoding="utf-8") as file_out:
            file_out.writelines(line + "\n" for line in summary_lines)
    else:
        for lstr in summary_lines:
            print(lstr)


def _generate_summary(input_files: SarifFileSet) -> List[str]:
    """
    For each severity level (in priority order): create a list of the errors of
    that severity, print out how many there are and then do some further analysis
    of which error codes are present.
    """
    ret = []
    report = input_files.get_report()
    for severity in report.get_severities():
        result_count = report.get_issue_count_for_severity(severity)
        issue_type_histogram = report.get_issue_type_histogram_for_severity(severity)
        ret.append(f"\n{severity}: {result_count}")
        ret += [f" - {key}: {count}" for (key, count) in issue_type_histogram.items()]
    filter_stats = input_files.get_filter_stats()
    if filter_stats:
        ret.append(f"\nResults were filtered by {filter_stats}")
    return ret
