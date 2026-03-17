from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.publishing.diagnostics.page_result import (
    PageDiagnosticsResult,
)


class PageDiagnostics(ClientValue):
    def __init__(self, results=None):
        self.Results = ClientValueCollection(PageDiagnosticsResult, results)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Publishing.Diagnostics.PageDiagnostics"
