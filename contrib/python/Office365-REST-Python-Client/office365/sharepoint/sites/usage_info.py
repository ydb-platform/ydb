from office365.runtime.client_value import ClientValue


class UsageInfo(ClientValue):
    """
    Provides fields used to access information regarding site collection usage.
    """

    def __init__(self, bandwidth=None, discussion_storage=None, visits=None):
        """
        :param long bandwidth: Contains the cumulative bandwidth used by the site collection on the previous day or
            on the last day that log files were processed, which is tracked by usage analysis code.
        :param long discussion_storage: Contains the amount of storage, identified in bytes,
            used by Web discussion data in the site collection.
        :param long visits: Contains the cumulative number of visits to the site collection,
            which is tracked by the usage analysis code.
        """
        super(UsageInfo, self).__init__()
        self.Bandwidth = bandwidth
        self.DiscussionStorage = discussion_storage
        self.Visits = visits
