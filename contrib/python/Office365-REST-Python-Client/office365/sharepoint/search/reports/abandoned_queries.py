from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.search.reports.abandonedqueries.item import (
    ReportAbandonedQueriesItem,
)
from office365.sharepoint.search.reports.base import ReportBase


class ReportAbandonedQueries(ReportBase):
    """This report shows popular search queries that receive low click-through. Use this report to identify search
    queries that might create user dissatisfaction and to improve the discoverability of content.
    """

    def __init__(self, reports=None):
        super(ReportAbandonedQueries, self).__init__()
        self.Reports = ClientValueCollection(ReportAbandonedQueriesItem, reports)

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.ReportAbandonedQueries"
