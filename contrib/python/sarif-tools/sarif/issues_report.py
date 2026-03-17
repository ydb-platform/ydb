"""
A report derived from a SARIF file or group of SARIF files.

The issues are grouped by severity, then by key (which is either issue code + truncated
description or just issue code if the issues have distinct descriptions), then listed in location
order.
"""

from typing import Dict, List

from sarif.sarif_file_utils import (
    combine_code_and_description,
    combine_record_code_and_description,
    record_sort_key,
    SARIF_SEVERITIES_WITHOUT_NONE,
    SARIF_SEVERITIES_WITH_NONE,
)


class IssuesReport:
    """
    This class imposes a hierarchical structure on a list of records which is helpful
    for presenting reader-friendly summaries.
    """

    def __init__(self):
        self._sev_to_records = {sev: [] for sev in SARIF_SEVERITIES_WITH_NONE}
        self._sev_to_sorted_keys = None
        self._records_have_been_sorted = False

    def add_record(self, record: dict):
        """Append record to list for severity - no sorting."""
        self._sev_to_records.setdefault(record["Severity"], []).append(record)
        if self._records_have_been_sorted:
            self._sev_to_sorted_keys = None
            self._records_have_been_sorted = False

    def _group_records_by_key(self):
        self._sev_to_sorted_keys = {}
        code_to_key_and_count = {}
        for severity, issues in self._sev_to_records.items():
            code_to_key_and_count.clear()
            for record in issues:
                code = record["Code"]
                key = combine_record_code_and_description(record)
                key_and_count = code_to_key_and_count.get(code)
                if key_and_count is None:
                    code_to_key_and_count[code] = {
                        "key": key,
                        "common_desc": record["Description"],
                        "count": 1,
                    }
                else:
                    key_and_count["count"] += 1
                    common_desc_stem = key_and_count["common_desc"]
                    desc = record["Description"]
                    if not desc.startswith(common_desc_stem):
                        for char_pos, (char1, char2) in enumerate(
                            zip(common_desc_stem, desc)
                        ):
                            if char1 != char2:
                                new_desc_stem = common_desc_stem[0:char_pos]
                                key_and_count["common_desc"] = new_desc_stem
                                key_and_count["key"] = combine_code_and_description(
                                    code, new_desc_stem + " ..."
                                )
                                break
            sorted_codes = sorted(
                code_to_key_and_count.keys(),
                key=lambda code: code_to_key_and_count[code]["count"],
                reverse=True,
            )
            self._sev_to_sorted_keys[severity] = {
                code_to_key_and_count[code]["key"]: [] for code in sorted_codes
            }
            for record in issues:
                # Not sorting the issues by location at this point
                code = record["Code"]
                self._sev_to_sorted_keys[severity][
                    code_to_key_and_count[code]["key"]
                ].append(record)

    def _sort_record_lists(self):
        if self._sev_to_sorted_keys is None:
            self._group_records_by_key()
        for key_to_records in self._sev_to_sorted_keys.values():
            for records in key_to_records.values():
                records.sort(key=record_sort_key)
        self._records_have_been_sorted = True

    def get_issue_count_for_severity(self, severity: str) -> int:
        """Get the number of individual records at this severity level."""
        return len(self._sev_to_records.get(severity, []))

    def get_issue_type_count_for_severity(self, severity: str) -> int:
        """Get the number of distinct issue types at this severity level."""
        if self._sev_to_sorted_keys is None:
            self._group_records_by_key()
        return len(self._sev_to_sorted_keys.get(severity, []))

    def any_none_severities(self) -> bool:
        """Are there any records with severity level "none"?"""
        return bool(self._sev_to_records.get("none", {}))

    def get_severities(self) -> List[str]:
        """
        Get the list of relevant severity levels for these records.

        The returned list always includes "error", "warning" and "note", the standard SARIF severity
        levels for code issues.  The unusual severity level "none" is only included at the end if
        there are any records with severity "none".
        """
        return (
            SARIF_SEVERITIES_WITH_NONE
            if self.any_none_severities()
            else SARIF_SEVERITIES_WITHOUT_NONE
        )

    def get_issues_grouped_by_type_for_severity(
        self, severity: str
    ) -> Dict[str, List[dict]]:
        """
        Get a dict from issue type key to list of matching records at this severity level.

        Issue type keys are derived from the issue code and (common prefix of) description.
        """
        if not self._records_have_been_sorted:
            self._sort_record_lists()
        return self._sev_to_sorted_keys.get(severity, {})

    def get_issue_type_histogram_for_severity(self, severity: str) -> Dict[str, int]:
        """
        Get a dict from issue type key to number of matching records at this severity level.

        This is the same as `{k: len(v) for k, v in d.items()}` where
        `d = report.get_issues_grouped_by_type_for_severity(severity)`.
        """
        if self._sev_to_sorted_keys is None:
            self._group_records_by_key()
        return {
            key: len(records)
            for key, records in self.get_issues_grouped_by_type_for_severity(
                severity
            ).items()
        }

    def get_issues_for_severity(self, severity: str) -> List[dict]:
        """
        Get a flat list of the issues at this severity.

        The sorting is consistent with `get_issues_grouped_by_type`, but the issues are not grouped
        by type.
        """
        type_to_issues = self.get_issues_grouped_by_type_for_severity(severity)
        ret = []
        for issues_for_type in type_to_issues.values():
            ret.extend(issues_for_type)
        return ret
