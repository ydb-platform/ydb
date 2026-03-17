"""
SARIF file filtering functionality.
"""

import os
import re
from typing import Optional, List

import copy
import jsonpath_ng.ext
import yaml

from sarif import sarif_file_utils
from sarif.filter.filter_stats import FilterStats, load_filter_stats_from_json

# Commonly used properties can be specified using shortcuts
# instead of full JSON path
FILTER_SHORTCUTS = {
    "author": "properties.blame.author",
    "author-mail": "properties.blame.author-mail",
    "committer": "properties.blame.committer",
    "committer-mail": "properties.blame.committer-mail",
    "location": "locations[*].physicalLocation.artifactLocation.uri",
    "rule": "ruleId",
    "suppression": "suppressions[*].kind",
}

# Some properties can have specific shortcuts to make it easier to write filters
# For example a file location can be specified using wildcards
FIELDS_REGEX_SHORTCUTS = {"uri": {"**": ".*", "*": "[^/]*", "?": "."}}

# Default configuration for all filters
DEFAULT_CONFIGURATION = {
    "default-include": True,
    "check-line-number": True,
}


def _get_filter_function(filter_spec):
    """Return a filter function for the given specification."""
    if filter_spec:
        filter_len = len(filter_spec)
        if filter_len > 2 and filter_spec.startswith("/") and filter_spec.endswith("/"):
            regex = filter_spec[1:-1]
            return lambda value: re.search(regex, value, re.IGNORECASE)
        substring = filter_spec
        # substring can be empty, in this case "in" returns true
        # and only existence of the property checked.
        return lambda value: substring in value
    return lambda value: True


def _convert_glob_to_regex(property_name, property_value_spec):
    # skip if property_value_spec is a regex
    if property_value_spec and not (
        property_value_spec.startswith("/") and property_value_spec.endswith("/")
    ):
        # get last component of property name
        last_component = property_name.split(".")[-1]
        if last_component in FIELDS_REGEX_SHORTCUTS:
            shortcuts = FIELDS_REGEX_SHORTCUTS[last_component]
            regex = re.compile("|".join(map(re.escape, shortcuts.keys())))
            property_value_spec = regex.sub(
                lambda match: shortcuts[match.group(0)], property_value_spec
            )

            return f"/{property_value_spec}/"
    return property_value_spec


class PropertyFilter:
    """
    Class that represents a filter term ready for efficient use.
    """

    def __init__(self, prop_path, prop_value_spec, global_configuration):
        """
        Compile a filter property.  See README for the filter spec format.

        :param prop_path: JsonPath or preset.
        :param prop_value_spec: Value spec.
        :param global_configuration: Global configuration of the filter.
        """
        self.prop_path = prop_path
        resolved_prop_path = FILTER_SHORTCUTS.get(prop_path, prop_path)
        self.jsonpath_expr = jsonpath_ng.ext.parse(resolved_prop_path)

        # if prop_value_spec is a dict, update filter configuration from it
        if isinstance(prop_value_spec, dict):
            self.filter_configuration = copy.deepcopy(global_configuration)
            for config_key, config_value in prop_value_spec.items():
                if config_key != "value":
                    self.filter_configuration[config_key] = config_value
            # actual value for the filter is in "value" key
            prop_value_spec = prop_value_spec.get("value", "")
        else:
            self.filter_configuration = global_configuration
        value_spec = _convert_glob_to_regex(resolved_prop_path, prop_value_spec)
        self.filter_function = _get_filter_function(value_spec)


class MultiPropertyFilter:
    """
    Class representing a list of PropertyFilter objects.

    These are combined using AND to filter results.
    """

    def __init__(self, filter_spec: List[dict], global_filter_configuration: dict):
        """
        Initialise from a filter spec.

        See README for filter spec format.  It's a list of property paths and values to be
        combined with AND to form a filter.
        """
        self.filter_spec = filter_spec
        self.and_terms = [
            PropertyFilter(prop_path, prop_value_spec, global_filter_configuration)
            for prop_path, prop_value_spec in filter_spec.items()
        ]


def _compile_filters(
    filters: List[dict], global_filter_configuration: dict
) -> List[MultiPropertyFilter]:
    return [
        MultiPropertyFilter(filter_spec, global_filter_configuration)
        for filter_spec in filters
        if filter_spec
    ]


class GeneralFilter:
    """
    Class that implements filtering.
    """

    def __init__(self):
        self.filter_stats = None
        self.include_filters = {}
        self.apply_inclusion_filter = False
        self.exclude_filters = {}
        self.apply_exclusion_filter = False
        self.configuration = copy.deepcopy(DEFAULT_CONFIGURATION)

    def init_filter(
        self, filter_description, configuration, include_filters, exclude_filters
    ):
        """
        Initialise the filter with the given filter patterns.
        """
        self.filter_stats = FilterStats(filter_description)
        self.configuration.update(configuration)
        self.include_filters = _compile_filters(include_filters, self.configuration)
        self.apply_inclusion_filter = len(include_filters) > 0
        self.exclude_filters = _compile_filters(exclude_filters, self.configuration)
        self.apply_exclusion_filter = len(exclude_filters) > 0

    def rehydrate_filter_stats(self, dehydrated_filter_stats, filter_datetime):
        """
        Restore filter stats from the SARIF file directly,
        where they were recorded when the filter was previously run.

        Note that if init_filter is called,
        these rehydrated stats are discarded.
        """
        self.filter_stats = load_filter_stats_from_json(dehydrated_filter_stats)
        self.filter_stats.filter_datetime = filter_datetime

    def _zero_counts(self):
        if self.filter_stats:
            self.filter_stats.reset_counters()

    def _filter_append(self, filtered_results: List[dict], result: dict):
        # Remove any existing filter log on the result
        result.setdefault("properties", {}).pop("filtered", None)

        if self.apply_inclusion_filter:
            included_stats = self._filter_result(result, self.include_filters)
            if not included_stats["matchedFilter"]:
                # Result is excluded by dint of not being included
                self.filter_stats.filtered_out_result_count += 1
                return
        else:
            # no inclusion filters, mark the result as included so far
            included_stats = {"state": "included", "matchedFilter": []}

        if self.apply_exclusion_filter:
            excluded_stats = self._filter_result(result, self.exclude_filters)
            if excluded_stats["matchedFilter"]:
                self.filter_stats.filtered_out_result_count += 1
                return

        included_state = included_stats["state"]
        if included_state == "included":
            self.filter_stats.filtered_in_result_count += 1
        elif included_state == "noLineNumber":
            self.filter_stats.unconvincing_line_number_count += 1
        else:
            self.filter_stats.missing_property_count += 1
        included_stats["filter"] = self.filter_stats.filter_description
        result["properties"]["filtered"] = included_stats

        filtered_results.append(result)

    def _filter_result(self, result: dict, filters: List[MultiPropertyFilter]) -> dict:
        matched_filters = []
        warnings = []
        (_file_path, line_number) = sarif_file_utils.read_result_location(result)
        unconvincing_line_number = line_number == "1" or not line_number
        default_include_noprop = False

        if filters:
            # filters contain rules which treated as OR.
            # if any rule matches, the record is selected.
            for mpf in filters:
                # filter_spec contains rules which treated as AND.
                # all rules must match to select the record.
                matched = True
                for property_filter in mpf.and_terms:
                    if (
                        property_filter.filter_configuration.get(
                            "check-line-number", True
                        )
                        and unconvincing_line_number
                    ):
                        warnings.append(
                            f"Field '{property_filter.prop_path}' not checked due to "
                            "missing line number information"
                        )
                        continue
                    found_results = property_filter.jsonpath_expr.find(result)
                    if found_results:
                        value = found_results[0].value
                        if property_filter.filter_function(value):
                            continue
                    else:
                        # property to filter on is not found, or skipped due to invalid line number.
                        # if "default-include" is true, include the "result" with a warning.
                        if property_filter.filter_configuration.get(
                            "default-include", True
                        ):
                            warnings.append(
                                f"Field '{property_filter.prop_path}' is missing but "
                                "the result included as default-include is true"
                            )
                            default_include_noprop = True
                            continue
                    matched = False
                    break
                if matched:
                    matched_filters.append(mpf.filter_spec)
                    break

        stats = {
            "state": "included",
            "matchedFilter": matched_filters,
        }

        if warnings:
            stats.update(
                {
                    "state": "noProperty" if default_include_noprop else "noLineNumber",
                    "warnings": warnings,
                }
            )

        return stats

    def filter_results(self, results: List[dict]) -> List[dict]:
        """
        Apply this filter to a list of results,
        return the results that pass the filter
        and as a side-effect, update the filter stats.
        """
        if self.apply_inclusion_filter or self.apply_exclusion_filter:
            self._zero_counts()
            ret = []
            for result in results:
                self._filter_append(ret, result)
            return ret
        # No inclusion or exclusion patterns
        return results

    def get_filter_stats(self) -> Optional[FilterStats]:
        """
        Get the statistics from running this filter.
        """
        return self.filter_stats


def load_filter_file(file_path):
    """
    Load a YAML filter file, return the filter description and the filters.
    """
    try:
        file_name = os.path.basename(file_path)
        with open(file_path, encoding="utf-8") as file_in:
            yaml_content = yaml.safe_load(file_in)
            filter_description = yaml_content.get("description", file_name)
            configuration = yaml_content.get("configuration", {})
            include_filters = yaml_content.get("include", {})
            exclude_filters = yaml_content.get("exclude", {})
    except yaml.YAMLError as error:
        raise IOError(f"Cannot read filter file {file_path}") from error
    return filter_description, configuration, include_filters, exclude_filters
