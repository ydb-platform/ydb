"""
Code for `sarif codeclimate` command.
"""

import os
import json
import hashlib

from sarif.sarif_file import SarifFileSet

_SEVERITIES = {"none": "info", "note": "info", "warning": "minor", "error": "major"}


def generate(input_files: SarifFileSet, output: str, output_multiple_files: bool):
    """
    Generate a JSON file in Code Climate schema containing the list of issues from the SARIF files.
    See https://github.com/codeclimate/platform/blob/master/spec/analyzers/SPEC.md
    Gitlab usage guide - https://docs.gitlab.com/ee/ci/testing/code_quality.html#implement-a-custom-tool
    """
    output_file = output
    if output_multiple_files:
        for input_file in input_files:
            output_file_name = input_file.get_file_name_without_extension() + ".json"
            print(
                "Writing Code Climate JSON summary of",
                input_file.get_file_name(),
                "to",
                output_file_name,
            )
            _write_to_json(
                input_file.get_records(), os.path.join(output, output_file_name)
            )
            filter_stats = input_file.get_filter_stats()
            if filter_stats:
                print(f"  Results are filtered by {filter_stats}")
        output_file = os.path.join(output, "static_analysis_output.json")
    source_description = input_files.get_description()
    print(
        "Writing Code Climate JSON summary for",
        source_description,
        "to",
        os.path.basename(output_file),
    )
    _write_to_json(input_files.get_records(), output_file)
    filter_stats = input_files.get_filter_stats()
    if filter_stats:
        print(f"  Results are filtered by {filter_stats}")


def _write_to_json(list_of_errors, output_file):
    """
    Write out the errors to a JSON file according to Code Climate specification.
    """
    content = []
    for record in list_of_errors:
        severity = _SEVERITIES.get(record.get("Severity", "warning"), "minor")

        # split Code value to extract error ID and description
        rule = record["Code"]
        description = record["Description"]

        path = record["Location"]
        line = record["Line"]

        fingerprint = hashlib.md5(
            f"{description} {path} ${line}`]".encode()
        ).hexdigest()

        # "categories" property is not used in GitLab but marked as "required" in Code Climate spec.
        # There is no easy way to determine a category so the fixed value is set.
        content.append(
            {
                "type": "issue",
                "check_name": rule,
                "description": description,
                "categories": ["Bug Risk"],
                "location": {"path": path, "lines": {"begin": line}},
                "severity": severity,
                "fingerprint": fingerprint,
            }
        )

    with open(output_file, "w", encoding="utf-8") as file_out:
        json.dump(content, file_out, indent=4)
