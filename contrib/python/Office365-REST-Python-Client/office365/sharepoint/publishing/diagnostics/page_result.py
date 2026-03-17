from office365.runtime.client_value import ClientValue


class PageDiagnosticsResult(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Publishing.Diagnostics.PageDiagnosticsResult"
