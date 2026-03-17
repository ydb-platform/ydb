"""
Statistics that record the outcome of a filter.
"""

import datetime


class FilterStats:
    """
    Statistics that record the outcome of a filter.
    """

    def __init__(self, filter_description):
        self.filter_description = filter_description
        # Filter stats can also be loaded from a file created by `sarif copy`.
        self.rehydrated = False
        self.filter_datetime = None
        self.filtered_in_result_count = 0
        self.filtered_out_result_count = 0
        self.missing_property_count = 0
        self.unconvincing_line_number_count = 0

    def reset_counters(self):
        """
        Zero all the counters.
        """
        self.filter_datetime = datetime.datetime.now()
        self.filtered_in_result_count = 0
        self.filtered_out_result_count = 0
        self.missing_property_count = 0
        self.unconvincing_line_number_count = 0

    def add(self, other_filter_stats):
        """
        Add another set of filter stats to my totals.
        """
        if other_filter_stats:
            if other_filter_stats.filter_description and (
                other_filter_stats.filter_description != self.filter_description
            ):
                self.filter_description += f", {other_filter_stats.filter_description}"
            self.filtered_in_result_count += other_filter_stats.filtered_in_result_count
            self.filtered_out_result_count += (
                other_filter_stats.filtered_out_result_count
            )
            self.missing_property_count += other_filter_stats.missing_property_count
            self.unconvincing_line_number_count += (
                other_filter_stats.unconvincing_line_number_count
            )

    def __str__(self):
        """
        Automatic to_string()
        """
        return self.to_string()

    def to_string(self):
        """
        Generate a summary string for these filter stats.
        """
        ret = f"'{self.filter_description}'"
        if self.filter_datetime:
            ret += " at "
            ret += self.filter_datetime.strftime("%c")
        ret += (
            f": {self.filtered_out_result_count} filtered out, "
            f"{self.filtered_in_result_count} passed the filter"
        )
        if self.unconvincing_line_number_count:
            ret += (
                f", {self.unconvincing_line_number_count} included by default "
                "for lacking line number information"
            )
        if self.missing_property_count:
            ret += (
                f", {self.missing_property_count} included by default "
                "for lacking data to filter"
            )

        return ret

    def to_json_camel_case(self):
        """
        Generate filter stats as JSON using camelCase naming,
        to fit with SARIF standard section 3.8.1 (Property Bags).
        """
        return {
            "filter": self.filter_description,
            "in": self.filtered_in_result_count,
            "out": self.filtered_out_result_count,
            "default": {
                "noProperty": self.missing_property_count,
                "noLineNumber": self.unconvincing_line_number_count,
            },
        }


def load_filter_stats_from_json(json_data):
    """
    Load filter stats from a SARIF file property bag using camelCase naming
    as per SARIF standard section 3.8.1 (Property Bags).
    """
    ret = None
    if json_data:
        ret = FilterStats(json_data["filter"])
        ret.rehydrated = True
        ret.filtered_in_result_count = json_data.get("in", 0)
        ret.filtered_out_result_count = json_data.get("out", 0)
        default_stats = json_data.get("default", {})
        ret.unconvincing_line_number_count = default_stats.get("noLineNumber", 0)
        ret.missing_property_count = default_stats.get("noProperty", 0)
    return ret
