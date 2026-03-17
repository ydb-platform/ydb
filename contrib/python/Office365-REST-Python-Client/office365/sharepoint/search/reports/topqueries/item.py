from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.search.reports.topqueries.data import ReportTopQueriesData


class ReportTopQueriesItem(ClientValue):
    """ """

    def __init__(self, date=None, report=None):
        self.Date = date
        self.Report = ClientValueCollection(ReportTopQueriesData, report)

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.ReportTopQueriesItem"
