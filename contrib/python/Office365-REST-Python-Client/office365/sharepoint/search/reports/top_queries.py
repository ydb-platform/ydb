from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.search.reports.base import ReportBase
from office365.sharepoint.search.reports.topqueries.item import ReportTopQueriesItem


class ReportTopQueries(ReportBase):

    def __init__(self, reports=None):
        super(ReportTopQueries, self).__init__()
        self.Reports = ClientValueCollection(ReportTopQueriesItem, reports)

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.ReportTopQueries"
