from office365.runtime.client_value import ClientValue


class TenantCustomQuerySuggestions(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Query.TenantCustomQuerySuggestions"
