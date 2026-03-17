from office365.runtime.client_value import ClientValue


class SharePointHomePageContext(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.Home.SharePointHomePageContext"
