from office365.runtime.client_value import ClientValue


class PopularTenantQuery(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Query.PopularTenantQuery"
