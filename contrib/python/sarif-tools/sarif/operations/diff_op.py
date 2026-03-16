"""
Code for `sarif diff` command.
"""

import json
import sys
from typing import Dict

from sarif import sarif_file


def _occurrences(occurrence_count):
    return (
        "1 occurrence" if occurrence_count == 1 else f"{occurrence_count} occurrences"
    )


def _signed_change(difference):
    return str(difference) if difference < 0 else f"+{difference}"


def _record_to_location_tuple(record) -> str:
    return (record["Location"], record["Line"])


def print_diff(
    old_sarif: sarif_file.SarifFileSet,
    new_sarif: sarif_file.SarifFileSet,
    output,
    check_level=None,
) -> int:
    """
    Generate a diff of the issues from the SARIF files and write it to stdout
    or a file if specified.
    :param old_sarif: corresponds to the old files.
    :param new_sarif: corresponds to the new files.
    :return: number of increased severities, or 0 if nothing has worsened.
    """
    diff = _calc_diff(old_sarif, new_sarif)
    if output:
        print("writing diff to", output)
        with open(output, "w", encoding="utf-8") as output_file:
            json.dump(diff, output_file, indent=4)
    else:
        for severity in sarif_file.SARIF_SEVERITIES_WITH_NONE:
            if severity not in diff:
                continue
            if diff[severity]["codes"]:
                print(
                    severity,
                    "level:",
                    _signed_change(diff[severity]["+"]),
                    _signed_change(-diff[severity]["-"]),
                )
                for issue_key, code_info in diff[severity]["codes"].items():
                    (old_count, new_count, new_locations) = (
                        code_info["<"],
                        code_info[">"],
                        code_info.get("+@", []),
                    )
                    if old_count == 0:
                        print(f'  New issue "{issue_key}" ({_occurrences(new_count)})')
                    elif new_count == 0:
                        print(f'  Eliminated issue "{issue_key}"')
                    else:
                        print(
                            f"  Number of occurrences {old_count} -> {new_count}",
                            f'({_signed_change(new_count - old_count)}) for issue "{issue_key}"',
                        )
                    if new_locations:
                        # Print the top 3 new locations
                        for record in new_locations[0:3]:
                            (location, line) = _record_to_location_tuple(record)
                            print(f"    {location}:{line}")
                        if len(new_locations) > 3:
                            print("    ...")
            else:
                print(severity, "level: +0 -0 no changes")
        print(
            "all levels:",
            _signed_change(diff["all"]["+"]),
            _signed_change(-diff["all"]["-"]),
        )
    filter_stats = old_sarif.get_filter_stats()
    if filter_stats:
        print(f"  'Before' results were filtered by {filter_stats}")
    filter_stats = new_sarif.get_filter_stats()
    if filter_stats:
        print(f"  'After' results were filtered by {filter_stats}")
    ret = 0
    if check_level:
        for severity in sarif_file.SARIF_SEVERITIES_WITH_NONE:
            ret += diff.get(severity, {}).get("+", 0)
            if severity == check_level:
                break
    if ret > 0:
        sys.stderr.write(
            f"Check: exiting with return code {ret} due to increase in issues at or above {check_level} severity\n"
        )
    return ret


def _find_new_occurrences(new_records, old_records):
    # Note: this is O(n²) complexity where n is the number of occurrences of this issue type,
    # so could be slow when there are a large number of occurrences.
    old_occurrences = old_records
    new_occurrences_new_locations = []
    new_occurrences_new_lines = []
    for new_record in new_records:
        (new_location, new_line) = (True, True)
        for old_record in old_occurrences:
            if old_record["Location"] == new_record["Location"]:
                new_location = False
                if old_record["Line"] == new_record["Line"]:
                    new_line = False
                    break
        if new_location:
            if new_record not in new_occurrences_new_locations:
                new_occurrences_new_locations.append(new_record)
        elif new_line:
            if new_record not in new_occurrences_new_lines:
                new_occurrences_new_lines.append(new_record)

    return sorted(
        new_occurrences_new_locations, key=_record_to_location_tuple
    ) + sorted(new_occurrences_new_lines, key=_record_to_location_tuple)


def _calc_diff(
    old_sarif: sarif_file.SarifFileSet, new_sarif: sarif_file.SarifFileSet
) -> Dict:
    """
    Generate a diff of the issues from the SARIF files.
    old_sarif corresponds to the old files.
    new_sarif corresponds to the new files.
    Return dict has keys "error", "warning", "note", "none" (if present) and "all".
    """
    ret = {"all": {"+": 0, "-": 0}}
    old_report = old_sarif.get_report()
    new_report = new_sarif.get_report()
    # Include `none` in the list of severities if there are any `none` records in either the old
    # or new report.
    severities = (
        old_report.get_severities()
        if old_report.any_none_severities()
        else new_report.get_severities()
    )
    for severity in severities:
        old_histogram = old_report.get_issue_type_histogram_for_severity(severity)
        new_histogram = new_report.get_issue_type_histogram_for_severity(severity)
        ret[severity] = {"+": 0, "-": 0, "codes": {}}
        if old_histogram != new_histogram:
            for issue_key, count in new_histogram.items():
                old_count = old_histogram.pop(issue_key, 0)
                if old_count != count:
                    ret[severity]["codes"][issue_key] = {"<": old_count, ">": count}
                    if old_count == 0:
                        ret[severity]["+"] += 1
                    new_occurrences = _find_new_occurrences(
                        new_report.get_issues_grouped_by_type_for_severity(
                            severity
                        ).get(issue_key, []),
                        old_report.get_issues_grouped_by_type_for_severity(
                            severity
                        ).get(issue_key, []),
                    )
                    if new_occurrences:
                        ret[severity]["codes"][issue_key]["+@"] = [
                            {"Location": r["Location"], "Line": r["Line"]}
                            for r in new_occurrences
                        ]
            for issue_key, old_count in old_histogram.items():
                ret[severity]["codes"][issue_key] = {"<": old_count, ">": 0}
                ret[severity]["-"] += 1
        ret["all"]["+"] += ret[severity]["+"]
        ret["all"]["-"] += ret[severity]["-"]
    return ret
