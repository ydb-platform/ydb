from office365.runtime.client_value import ClientValue


class AggregationOption(ClientValue):
    """Specifies which aggregations should be returned alongside the search results.
    The maximum returned value is 100 buckets."""
