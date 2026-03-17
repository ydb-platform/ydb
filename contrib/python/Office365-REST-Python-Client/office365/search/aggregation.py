from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.search.bucket import SearchBucket


class SearchAggregation(ClientValue):
    """Provides the details of the search aggregation in the search response."""

    def __init__(self, buckets=None, field=None):
        """
        :param list[SearchBucket] buckets: Defines the actual buckets of the computed aggregation.
        :param str field: Defines on which field the aggregation was computed on.
        """
        self.buckets = ClientValueCollection(SearchBucket, buckets)
        self.field = field
