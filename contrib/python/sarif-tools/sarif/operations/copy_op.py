"""
Code for `sarif copy` command.
"""

import copy
import datetime
import json
import os

from sarif import loader, sarif_file
from sarif.sarif_file import SarifFileSet, SarifFile


def generate_sarif(
    input_files: SarifFileSet,
    output: str,
    append_timestamp: bool,
    sarif_tools_version: str,
    cmdline: str,
) -> SarifFile:
    """
    Generate a new SARIF file based on the input files
    """
    sarif_data_out = {
        "$schema": "https://schemastore.azurewebsites.net/schemas/json/sarif-2.1.0-rtm.5.json",
        "version": "2.1.0",
        "runs": [],
    }
    now = datetime.datetime.now(datetime.timezone.utc)
    output_file_abs_path = os.path.abspath(output)
    conversion_timestamp_iso8601 = now.isoformat()
    conversion_timestamp_trendformat = now.strftime(sarif_file.DATETIME_FORMAT)
    run_count = 0
    input_file_count = 0
    for input_file in input_files:
        if input_file.get_abs_file_path() == output_file_abs_path:
            print(f"Auto-excluding output file {output} from input file list")
            continue
        input_file_count += 1
        input_file_path = input_file.get_abs_file_path()
        input_file_modified_iso8601 = input_file.mtime.isoformat()
        for input_run in input_file.runs:
            run_count += 1
            # Create a shallow copy
            input_run_json_copy = copy.copy(input_run.run_data)
            conversion_properties = {
                "file": input_file_path,
                "modified": input_file_modified_iso8601,
                "processed": conversion_timestamp_iso8601,
            }
            input_run_json_copy["conversion"] = {
                "tool": {
                    "driver": {
                        "name": "sarif-tools",
                        "fullName": "sarif-tools https://github.com/microsoft/sarif-tools/",
                        "version": sarif_tools_version,
                        "properties": conversion_properties,
                    }
                },
                "invocation": {"commandLine": cmdline, "executionSuccessful": True},
            }
            results = input_run.get_results()
            filter_stats = input_run.get_filter_stats()
            if filter_stats:
                input_run_json_copy["results"] = results
                conversion_properties["filtered"] = filter_stats.to_json_camel_case()
            sarif_data_out["runs"].append(input_run_json_copy)
    output_file_path = output
    if append_timestamp:
        output_split = os.path.splitext(output)
        output_file_path = (
            output_split[0]
            + f"_{conversion_timestamp_trendformat}"
            + (output_split[1] or ".sarif")
        )
    with open(output_file_path, "w", encoding="utf-8") as file_out:
        json.dump(sarif_data_out, file_out, indent=4)
    runs_string = "1 run" if run_count == 1 else f"{run_count} runs"
    files_string = (
        "1 SARIF file" if input_file_count == 1 else f"{input_file_count} SARIF files"
    )
    print(f"Wrote {output_file_path} with {runs_string} from {files_string}")
    total_filter_stats = input_files.get_filter_stats()
    if total_filter_stats:
        print(total_filter_stats.to_string())
    return loader.load_sarif_file(output_file_path)
