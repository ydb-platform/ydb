from office365.runtime.client_value import ClientValue


class SearchBucket(ClientValue):
    """Represents a container for one or more search results that share the same value for the entity field
    that aggregates them."""

    def __init__(self, aggregation_filter_token=None, count=None, key=None):
        """
        :param str aggregation_filter_token: A token containing the encoded filter to aggregate search matches by the
           specific key value. To use the filter, pass the token as part of the aggregationFilter property in a
           searchRequest object, in the format "{field}:\"{aggregationFilterToken}\""
        :param int count: The approximate number of search matches that share the same value specified in the
           key property. Note that this number is not the exact number of matches.
        :param str key: The discrete value of the field that an aggregation was computed on.
        """
        self.aggregationFilterToken = aggregation_filter_token
        self.count = count
        self.key = key
