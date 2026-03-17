from office365.runtime.client_value import ClientValue


class ReportAbandonedQueriesItem(ClientValue):

    def __init__(self, date=None):
        self.Date = date

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.ReportAbandonedQueriesItem"
