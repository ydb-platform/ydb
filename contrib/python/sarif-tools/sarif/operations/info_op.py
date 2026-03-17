"""
Code for `sarif info` command.
"""

import datetime
import os

from sarif.sarif_file import SarifFileSet

_BYTES_PER_MIB = 1024 * 1024
_BYTES_PER_KIB = 1024


def _property_bag_stats(object_list):
    tally = {}
    universal_property_keys = []
    partial_properties = []
    if object_list:
        for obj in object_list:
            for key in obj.get("properties", {}):
                tally[key] = tally[key] + 1 if key in tally else 1
        object_count = len(object_list)
        universal_property_keys = [
            key for (key, count) in tally.items() if count == object_count
        ]

        def tally_rank(key_count_pair):
            # Sort by descending tally then alphabetically
            return (-key_count_pair[1], key_count_pair[0])

        partial_properties = [
            {"key": key, "count": count, "percent": 100 * count / object_count}
            for (key, count) in sorted(tally.items(), key=tally_rank)
            if count < object_count
        ]
    return universal_property_keys, partial_properties


def _generate_info_to_file(sarif_files, file_out):
    file_count = False
    for input_file in sarif_files:
        file_count += 1
        file_path = input_file.get_abs_file_path()
        file_stat = os.stat(file_path)
        size_in_bytes = file_stat.st_size
        if size_in_bytes > _BYTES_PER_MIB:
            readable_size = f"{file_stat.st_size / _BYTES_PER_MIB:.1f} MiB"
        else:
            readable_size = (
                f"{(file_stat.st_size + _BYTES_PER_KIB - 1) // _BYTES_PER_KIB} KiB"
            )
        print(input_file.get_abs_file_path(), file=file_out)
        print(f"  {file_stat.st_size} bytes ({readable_size})", file=file_out)
        print(
            f"  modified: {datetime.datetime.fromtimestamp(file_stat.st_mtime)}, "
            f"accessed: {datetime.datetime.fromtimestamp(file_stat.st_atime)}, "
            f"ctime: {datetime.datetime.fromtimestamp(file_stat.st_ctime)}",
            file=file_out,
        )
        run_count = len(input_file.runs)
        print(f"  {run_count} runs" if run_count != 1 else "  1 run", file=file_out)
        for run_index, run in enumerate(input_file.runs):
            if run_count != 1:
                print(f"  Run #{run_index + 1}:", file=file_out)
            print(f"    Tool: {run.get_tool_name()}", file=file_out)
            conversion_tool = run.get_conversion_tool_name()
            if conversion_tool:
                print(f"    Conversion tool: {conversion_tool}", file=file_out)
            results = run.get_results()
            result_count = len(results)
            print(
                f"    {result_count} results" if result_count != 1 else "    1 result",
                file=file_out,
            )
            universal_property_keys, partial_properties = _property_bag_stats(results)
            ppk_string = (
                ", ".join(
                    "{} {}/{} ({:.1f} %)".format(
                        p["key"], p["count"], result_count, p["percent"]
                    )
                    for p in partial_properties
                )
                if partial_properties
                else None
            )
            if universal_property_keys:
                upk_string = ", ".join(universal_property_keys)
                if partial_properties:
                    print(
                        f"    Result properties: all results have properties: {upk_string}; "
                        f"some results have properties: {ppk_string}",
                        file=file_out,
                    )
                else:
                    print(
                        f"    All results have properties: {upk_string}",
                        file=file_out,
                    )
            elif partial_properties:
                print(
                    f"    Result properties: {ppk_string}",
                    file=file_out,
                )
        print(file=file_out)
    return file_count


def generate_info(sarif_files: SarifFileSet, output: str):
    """
    Print structure information about the provided `sarif_files`.
    """
    if output:
        with open(output, "w", encoding="utf-8") as file_out:
            file_count = _generate_info_to_file(sarif_files, file_out)
        if file_count:
            files_string = (
                "1 SARIF file" if file_count == 1 else f"{file_count} SARIF files"
            )
            print("Wrote information about", files_string, "to", output)
    else:
        file_count = _generate_info_to_file(sarif_files, None)
    if file_count == 0:
        print(
            "No SARIF files found.  Try passing a path of a SARIF file or containing SARIF files."
        )
