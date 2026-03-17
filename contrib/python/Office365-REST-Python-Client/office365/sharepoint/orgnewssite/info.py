from office365.runtime.client_value import ClientValue


class OrgNewsSiteInfo(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.OrgNewsSite.OrgNewsSiteInfo"
