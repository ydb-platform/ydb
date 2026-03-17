"""
Defines classes representing sets of SARIF files, individual SARIF files and runs within SARIF
files, along with associated functions and constants.
"""

import copy
import datetime
import os
import re
from typing import Dict, Iterator, List, Optional

from sarif import sarif_file_utils
from sarif.sarif_file_utils import (
    SARIF_SEVERITIES_WITHOUT_NONE,
    SARIF_SEVERITIES_WITH_NONE,
    read_result_severity,
)
from sarif.filter.general_filter import GeneralFilter
from sarif.filter.filter_stats import FilterStats
from sarif.issues_report import IssuesReport

BASIC_RECORD_ATTRIBUTES = [
    "Tool",
    "Severity",
    "Code",
    "Description",
    "Location",
    "Line",
]
BLAME_RECORD_ATTRIBUTES = ["Author"]

# Standard time format for filenames, e.g. `20211012T110000Z`
# (not part of the SARIF standard).
# Can obtain from bash via `date +"%Y%m%dT%H%M%SZ"``
DATETIME_FORMAT = "%Y%m%dT%H%M%SZ"
DATETIME_REGEX = r"\d{8}T\d{6}Z"

_SLASHES = ["\\", "/"]


def has_sarif_file_extension(filename):
    """
    As per section 3.2 of the SARIF standard, SARIF filenames SHOULD end in ".sarif" and MAY end in
    ".sarif.json".
    https://docs.oasis-open.org/sarif/sarif/v2.1.0/os/sarif-v2.1.0-os.html#_Toc34317421
    """
    filename_upper = filename.upper().strip()
    return any(filename_upper.endswith(x) for x in [".SARIF", ".SARIF.JSON"])


def get_record_headings(with_blame) -> List[str]:
    """
    Get the record headings for this SARIF file.

    The result is BASIC_RECORD_ATTRIBUTES if there is no blame information in the SARIF file,
    or extended with author information otherwise.
    """
    return (
        BASIC_RECORD_ATTRIBUTES + BLAME_RECORD_ATTRIBUTES
        if with_blame
        else BASIC_RECORD_ATTRIBUTES
    )


def _add_filter_stats(accumulator, filter_stats):
    if filter_stats:
        if accumulator:
            accumulator.add(filter_stats)
            return accumulator
        return copy.copy(filter_stats)
    return accumulator


def _get_author_mail_from_blame_info(blame_info):
    return (
        blame_info.get("author-mail", None) or blame_info.get("committer-mail", None)
        if blame_info
        else None
    )


class SarifRun:
    """
    Class to hold a run object from a SARIF file (an entry in the top-level "runs" list
    in a SARIF file), as defined in SARIF standard section 3.14.
    https://docs.oasis-open.org/sarif/sarif/v2.1.0/os/sarif-v2.1.0-os.html#_Toc34317484
    """

    def __init__(self, sarif_file_object, run_index, run_data):
        self.sarif_file = sarif_file_object
        self.run_index = run_index
        self.run_data = run_data
        self._path_prefixes_upper = None
        self._cached_records = None
        self._filter = GeneralFilter()
        self._default_line_number = None
        conversion = run_data.get("conversion", None)
        if conversion:
            conversion_driver = conversion.get("tool", {}).get("driver", {})
            if conversion_driver.get("name", None) == "sarif-tools":
                # This run was written out by this tool!  Can restore filter stats.
                dehydrated_filter_stats = conversion_driver.get("properties", {}).get(
                    "filtered", None
                )
                if dehydrated_filter_stats:
                    filter_date = conversion_driver["properties"].get("processed", None)
                    self._filter.rehydrate_filter_stats(
                        dehydrated_filter_stats,
                        (
                            datetime.datetime.fromisoformat(filter_date)
                            if filter_date
                            else None
                        ),
                    )

    def init_path_prefix_stripping(self, autotrim=False, path_prefixes=None):
        """
        Set up path prefix stripping.  When records are subsequently obtained, the start of the
        path is stripped.
        If no path_prefixes are specified, the default behaviour is to strip the common prefix
        from each run.
        If path prefixes are specified, the specified prefixes are stripped.
        """
        prefixes = []
        if path_prefixes:
            prefixes = [prefix.strip().upper() for prefix in path_prefixes]
        if autotrim:
            autotrim_prefix = None
            records = self.get_records()
            if len(records) == 1:
                loc = records[0]["Location"].strip()
                slash_pos = max(loc.rfind(slash) for slash in _SLASHES)
                autotrim_prefix = loc[0:slash_pos] if slash_pos > -1 else None
            elif len(records) > 1:
                common_prefix = records[0]["Location"].strip()
                for record in records[1:]:
                    for char_pos, char in enumerate(record["Location"].strip()):
                        if char_pos >= len(common_prefix):
                            break
                        if char != common_prefix[char_pos]:
                            common_prefix = common_prefix[0:char_pos]
                            break
                    if not common_prefix:
                        break
                if common_prefix:
                    autotrim_prefix = common_prefix.upper()
            if autotrim_prefix and not any(
                p.startswith(autotrim_prefix.strip().upper()) for p in prefixes
            ):
                prefixes.append(autotrim_prefix)
        self._path_prefixes_upper = prefixes or None
        # Clear the untrimmed records cached by get_records() above.
        self._cached_records = None

    def init_default_line_number_1(self):
        """
        Some SARIF records lack a line number.  If this method is called, the default line number
        "1" is substituted in that case in the records returned by get_records().  Otherwise,
        None is returned.
        """
        self._default_line_number = "1"
        self._cached_records = None

    def init_general_filter(
        self, filter_description, configuration, include_filters, exclude_filters
    ):
        """
        Set up general filtering.  This is applied to all properties in results array in each SARIF file.
        If only inclusion criteria are provided, only issues matching the inclusion criteria are considered.
        If only exclusion criteria are provided, only issues not matching the exclusion criteria are considered.
        If both are provided, only issues matching the inclusion criteria and not matching the
        exclusion criteria are considered.
        """
        self._filter.init_filter(
            filter_description, configuration, include_filters, exclude_filters
        )
        # Clear the unfiltered records cached by get_records() above.
        self._cached_records = None

    def get_tool_name(self) -> str:
        """
        Get the tool name from this run.
        """
        return self.run_data["tool"]["driver"]["name"]

    def get_conversion_tool_name(self) -> Optional[str]:
        """
        Get the conversion tool name from this run, if any.
        """
        if "conversion" in self.run_data:
            return (
                self.run_data["conversion"]["tool"].get("driver", {}).get("name", None)
            )
        return None

    def get_rules_by_id(self, rule_id: str) -> List[Dict]:
        """
        Get the sarif rule for the given ID, if it exists in this run.
        """
        ret = []
        rule_id = rule_id.strip()
        if not rule_id:
            return ret
        for rule in self.run_data.get("tool", {}).get("driver", {}).get("rules", []):
            if rule.get("id", "") == rule_id:
                ret.append(rule)
        return ret

    def get_results(self) -> List[Dict]:
        """
        Get the results from this run.  These are the Result objects as defined in the SARIF
        standard section 3.27.  The results are filtered if a filter has ben configured.
        https://docs.oasis-open.org/sarif/sarif/v2.1.0/os/sarif-v2.1.0-os.html#_Toc34317638
        """
        return self._filter.filter_results(self.run_data["results"])

    def get_records(self) -> List[Dict]:
        """
        Get simplified records derived from the results of this run.  The records have the
        keys defined in `RECORD_ATTRIBUTES`.
        """
        if not self._cached_records:
            results = self.get_results()
            include_blame_info = self.has_blame_info()
            self._cached_records = [
                self.result_to_record(result, include_blame_info) for result in results
            ]
        return self._cached_records

    def get_report(self) -> IssuesReport:
        """Get the report, with records grouped and sorted for display."""
        ret = IssuesReport()
        for record in self.get_records():
            ret.add_record(record)
        return ret

    def result_to_record(self, result, include_blame_info=False):
        """
        Convert a SARIF result object to a simple record dict.

        Fields are "Tool", "Location", "Line", "Severity", "Code" and "Description".  Also "Author"
        if blame information is present.  See definition of result object here:
        https://docs.oasis-open.org/sarif/sarif/v2.1.0/os/sarif-v2.1.0-os.html#_Toc34317638
        """
        error_id = result["ruleId"]
        tool_name = self.get_tool_name()
        (file_path, line_number) = sarif_file_utils.read_result_location(result)
        if not file_path:
            # Result having non-empty location is only a "SHOULD" in the SARIF spec 3.27.12.
            # Some tools such as GCC 13 can output issues with no location.
            file_path = "-"
        if not line_number:
            line_number = "1"

        if self._path_prefixes_upper:
            file_path_upper = file_path.upper()
            for prefix in self._path_prefixes_upper:
                if file_path_upper.startswith(prefix):
                    prefixlen = len(prefix)
                    if len(file_path) > prefixlen and file_path[prefixlen] in _SLASHES:
                        # Strip off trailing path separator
                        file_path = file_path[prefixlen + 1 :]
                    else:
                        file_path = file_path[prefixlen:]
                    break

        severity = read_result_severity(result, self.run_data)

        if "message" in result:
            # per RFC3629 At least one of the text (§3.11.8) or id (§3.11.10) properties SHALL be present https://docs.oasis-open.org/sarif/sarif/v2.1.0/os/sarif-v2.1.0-os.html#RFC3629
            message_data = result["message"]
            if "text" in message_data:
                message = message_data["text"]
            elif "id" in message_data:
                message = message_data["id"]
            else:
                raise IOError(
                    "Message for result "
                    + error_id
                    + " from tool "
                    + tool_name
                    + " do not comply with RFC3629. At least one of the text (§3.11.8) or id (§3.11.10) properties SHALL be present https://docs.oasis-open.org/sarif/sarif/v2.1.0/os/sarif-v2.1.0-os.html#RFC3629"
                )
        else:
            message = error_id

        # Create a dict representing this result
        record = {
            "Tool": tool_name,
            "Location": file_path,
            "Line": line_number,
            "Severity": severity,
            "Code": error_id,
            "Description": (
                message[len(error_id) + 1].strip()
                if message.startswith(error_id) and len(message) > len(error_id) + 1
                else message
            ),
        }
        if include_blame_info:
            record["Author"] = _get_author_mail_from_blame_info(
                result.get("properties", {}).get("blame", None)
            )

        return record

    def get_result_count(self) -> int:
        """
        Return the total number of results.
        """
        return len(self.get_results())

    def get_filter_stats(self) -> Optional[FilterStats]:
        """
        Get the number of records that were included or excluded by the filter.
        """
        return self._filter.get_filter_stats()

    def has_blame_info(self) -> bool:
        """
        Check whether this SARIF file contains blame info.
        """
        return any(
            r.get("properties", {}).get("blame", None)
            for r in self.run_data.get("results", [])
        )

    def get_record_headings(self) -> List[str]:
        """
        Get the record headings for this SARIF file.

        The result is BASIC_RECORD_ATTRIBUTES if there is no blame information in the SARIF file,
        or extended with author information otherwise.
        """
        return (
            BASIC_RECORD_ATTRIBUTES + BLAME_RECORD_ATTRIBUTES
            if self.has_blame_info()
            else BASIC_RECORD_ATTRIBUTES
        )

    def any_none(self) -> bool:
        """Are there any records with severity "none"?"""
        return any(
            record.get("Severity", "warning") == "none" for record in self.get_records()
        )

    def get_severities(self, include_none=False) -> List[str]:
        """
        Get the severity list, with "none" omitted unless present or force-included.

        Return ["error", "warning", "note", "none"] if include_none or if there
        are any records with severity "none", otherwise ["error", "warning", "note"].
        """
        if include_none or self.any_none():
            return SARIF_SEVERITIES_WITH_NONE
        return SARIF_SEVERITIES_WITHOUT_NONE


class SarifFile:
    """
    Class to hold SARIF data parsed from a file and provide accessors to the data.
    """

    def __init__(self, file_path, data, mtime=None):
        self.abs_file_path = os.path.abspath(file_path)
        if mtime:
            self.mtime = mtime
        else:
            stat = os.stat(file_path)
            self.mtime = datetime.datetime.fromtimestamp(stat.st_mtime)
        self.data = data
        self.runs = [
            SarifRun(self, run_index, run_data)
            for (run_index, run_data) in enumerate(self.data.get("runs", []))
        ]

    def __bool__(self):
        """
        True if non-empty.
        """
        return bool(self.runs)

    def init_path_prefix_stripping(self, autotrim=False, path_prefixes=None):
        """
        Set up path prefix stripping.  When records are subsequently obtained, the start of the
        path is stripped.
        If no path_prefixes are specified, the default behaviour is to strip the common prefix
        from each run.
        If path prefixes are specified, the specified prefixes are stripped.
        """
        for run in self.runs:
            run.init_path_prefix_stripping(autotrim, path_prefixes)

    def init_default_line_number_1(self):
        """
        Some SARIF records lack a line number.  If this method is called, the default line number
        "1" is substituted in that case in the records returned by get_records().  Otherwise,
        None is returned.
        """
        for run in self.runs:
            run.init_default_line_number_1()

    def init_general_filter(
        self, filter_description, configuration, include_filters, exclude_filters
    ):
        """
        Set up general filtering.  This is applied to all properties in results array in each SARIF file.
        If only inclusion criteria are provided, only issues matching the inclusion criteria are considered.
        If only exclusion criteria are provided, only issues not matching the exclusion criteria are considered.
        If both are provided, only issues matching the inclusion criteria and not matching the
        exclusion criteria are considered.
        """
        for run in self.runs:
            run.init_general_filter(
                filter_description, configuration, include_filters, exclude_filters
            )

    def get_abs_file_path(self) -> str:
        """
        Get the absolute file path from which this SARIF data was loaded.
        """
        return self.abs_file_path

    def get_file_name(self) -> str:
        """
        Get the file name from which this SARIF data was loaded.
        """
        return os.path.basename(self.abs_file_path)

    def get_file_name_without_extension(self) -> str:
        """
        Get the file name from which this SARIF data was loaded, without extension.
        """
        file_name = self.get_file_name()
        return file_name[0 : file_name.index(".")] if "." in file_name else file_name

    def get_file_name_extension(self) -> str:
        """
        Get the extension of the file name from which this SARIF data was loaded.
        Initial "." exlcuded.
        """
        file_name = self.get_file_name()
        return file_name[file_name.index(".") + 1 :] if "." in file_name else ""

    def get_filename_timestamp(self) -> str:
        """
        Extract the timestamp from the filename and return the date-time string extracted.
        """
        parsed_date = re.findall(DATETIME_REGEX, self.get_file_name())
        return parsed_date if len(parsed_date) == 1 else None

    def get_distinct_tool_names(self):
        """
        Return a list of tool names that feature in the runs in this file.
        The list is deduplicated and sorted into alphabetical order.
        """
        return sorted(list(set(run.get_tool_name() for run in self.runs)))

    def get_rules_by_id(self, rule_id: str) -> List[Dict]:
        """
        Get the sarif rule(s) for the given ID.
        """
        ret = []
        for run in self.runs:
            ret.extend(run.get_rules_by_id(rule_id))
        return ret

    def get_results(self) -> List[Dict]:
        """
        Get the results from all runs in this file.  These are the Result objects as defined in the
        SARIF standard section 3.27.
        https://docs.oasis-open.org/sarif/sarif/v2.1.0/os/sarif-v2.1.0-os.html#_Toc34317638
        """
        ret = []
        for run in self.runs:
            ret += run.get_results()
        return ret

    def get_records(self) -> List[Dict]:
        """
        Get simplified records derived from the results of all runs.  The records have the
        keys defined in `RECORD_ATTRIBUTES`.
        """
        ret = []
        for run in self.runs:
            ret += run.get_records()
        return ret

    def get_report(self) -> IssuesReport:
        """Get the report, with records grouped and sorted for display."""
        ret = IssuesReport()
        for record in self.get_records():
            ret.add_record(record)
        return ret

    def get_result_count(self) -> int:
        """
        Return the total number of results.
        """
        return sum(run.get_result_count() for run in self.runs)

    def get_filter_stats(self) -> Optional[FilterStats]:
        """
        Get the number of records that were included or excluded by the filter.
        """
        ret = None
        for run in self.runs:
            ret = _add_filter_stats(ret, run.get_filter_stats())
        return ret

    def has_blame_info(self) -> bool:
        """Is there (any) blame info in this SARIF file?"""
        return any(run.has_blame_info() for run in self.runs)

    def any_none(self) -> bool:
        """Are there any records with severity "none"?"""
        return any(run.any_none() for run in self.runs)

    def get_severities(self, include_none=False) -> List[str]:
        """
        Get the severity list, with "none" omitted unless present or force-included.

        Return ["error", "warning", "note", "none"] if include_none or if there
        are any records with severity "none", otherwise ["error", "warning", "note"].
        """
        if include_none or self.any_none():
            return SARIF_SEVERITIES_WITH_NONE
        return SARIF_SEVERITIES_WITHOUT_NONE


class SarifFileSet:
    """
    Class representing a set of SARIF files.
    The "composite" pattern is used to allow multiple subdirectories.
    """

    def __init__(self):
        self.subdirs = []
        self.files = []

    def __bool__(self):
        """
        Return true if there are any SARIF files, regardless of whether they contain any runs.
        """
        return any(bool(subdir) for subdir in self.subdirs) or bool(self.files)

    def __len__(self):
        """
        Return the number of SARIF files, in total.
        """
        return sum(len(subdir) for subdir in self.subdirs) + sum(
            1 for f in self.files if f
        )

    def __iter__(self) -> Iterator[SarifFile]:
        """
        Iterate the SARIF files in this set.
        """
        for subdir in self.subdirs:
            yield from subdir.files
        yield from self.files

    def __getitem__(self, index) -> SarifFile:
        i = 0
        for subdir in self.subdirs:
            for input_file in subdir.files:
                if i == index:
                    return input_file
                i += 1
        return self.files[index - i]

    def get_description(self):
        """
        Get a description of the SARIF file set - the name of the single file or the number of
        files.
        """
        count = len(self)
        if count == 1:
            return self[0].get_file_name()
        return f"{count} files"

    def init_path_prefix_stripping(self, autotrim=False, path_prefixes=None):
        """
        Set up path prefix stripping.  When records are subsequently obtained, the start of the
        path is stripped.
        If no path_prefixes are specified, the default behaviour is to strip the common prefix
        from each run.
        If path prefixes are specified, the specified prefixes are stripped.
        """
        for subdir in self.subdirs:
            subdir.init_path_prefix_stripping(autotrim, path_prefixes)
        for input_file in self.files:
            input_file.init_path_prefix_stripping(autotrim, path_prefixes)

    def init_default_line_number_1(self):
        """
        Some SARIF records lack a line number.  If this method is called, the default line number
        "1" is substituted in that case in the records returned by get_records().  Otherwise,
        None is returned.
        """
        for subdir in self.subdirs:
            subdir.init_default_line_number_1()
        for input_file in self.files:
            input_file.init_default_line_number_1()

    def init_general_filter(
        self, filter_description, configuration, include_filters, exclude_filters
    ):
        """
        Set up general filtering.  This is applied to all properties in results array in each SARIF file.
        If only inclusion criteria are provided, only issues matching the inclusion criteria are considered.
        If only exclusion criteria are provided, only issues not matching the exclusion criteria are considered.
        If both are provided, only issues matching the inclusion criteria and not matching the
        exclusion criteria are considered.
        """
        for subdir in self.subdirs:
            subdir.init_general_filter(
                filter_description, configuration, include_filters, exclude_filters
            )
        for input_file in self.files:
            input_file.init_general_filter(
                filter_description, configuration, include_filters, exclude_filters
            )

    def add_dir(self, sarif_file_set):
        """
        Add a SarifFileSet as a subdirectory.
        """
        self.subdirs.append(sarif_file_set)

    def add_file(self, sarif_file_object: SarifFile):
        """
        Add a single SARIF file to the set.
        """
        self.files.append(sarif_file_object)

    def get_distinct_tool_names(self) -> List[str]:
        """
        Return a list of tool names that feature in the runs in these files.
        The list is deduplicated and sorted into alphabetical order.
        """
        all_tool_names = set()
        for subdir in self.subdirs:
            all_tool_names.update(subdir.get_distinct_tool_names())
        for input_file in self.files:
            all_tool_names.update(input_file.get_distinct_tool_names())

        return sorted(list(all_tool_names))

    def get_rules_by_id(self, rule_id: str) -> List[Dict]:
        """
        Get the sarif rule(s) for the given ID.
        """
        ret = []
        for subdir in self.subdirs:
            ret.extend(subdir.get_rules_by_id(rule_id))
        for input_file in self.files:
            ret.extend(input_file.get_rules_by_id(rule_id))
        return ret

    def get_results(self) -> List[Dict]:
        """
        Get the results from all runs in all files.  These are the Result objects as defined in the
        SARIF standard section 3.27.
        https://docs.oasis-open.org/sarif/sarif/v2.1.0/os/sarif-v2.1.0-os.html#_Toc34317638
        """
        ret = []
        for subdir in self.subdirs:
            ret += subdir.get_results()
        for input_file in self.files:
            ret += input_file.get_results()
        return ret

    def get_records(self) -> List[Dict]:
        """
        Get simplified records derived from the results of all runs.  The records have the
        keys defined in `RECORD_ATTRIBUTES`.
        """
        ret = []
        for subdir in self.subdirs:
            ret += subdir.get_records()
        for input_file in self.files:
            ret += input_file.get_records()
        return ret

    def get_result_count(self) -> int:
        """
        Return the total number of results.
        """
        return sum(subdir.get_result_count() for subdir in self.subdirs) + sum(
            input_file.get_result_count() for input_file in self.files
        )

    def get_report(self) -> IssuesReport:
        """Get the report, with records grouped and sorted for display."""
        ret = IssuesReport()
        for record in self.get_records():
            ret.add_record(record)
        return ret

    def get_filter_stats(self) -> Optional[FilterStats]:
        """
        Get the number of records that were included or excluded by the filter.
        """
        ret = None
        for subdir in self.subdirs:
            ret = _add_filter_stats(ret, subdir.get_filter_stats())
        for input_file in self.files:
            ret = _add_filter_stats(ret, input_file.get_filter_stats())
        return ret

    def has_blame_info(self) -> bool:
        """Is there (any) blame info in (any of) the SARIF files?"""
        return any(input_file.has_blame_info() for input_file in self.files)

    def any_none(self) -> bool:
        """Are there any records with severity "none"?"""
        for subdir in self.subdirs:
            if subdir.any_none():
                return True
        for input_file in self.files:
            if input_file.any_none():
                return True
        return False

    def get_severities(self, include_none=False) -> List[str]:
        """
        Get the severity list, with "none" omitted unless present or force-included.

        Return ["error", "warning", "note", "none"] if include_none or if there
        are any records with severity "none", otherwise ["error", "warning", "note"].
        """
        if include_none or self.any_none():
            return SARIF_SEVERITIES_WITH_NONE
        return SARIF_SEVERITIES_WITHOUT_NONE
