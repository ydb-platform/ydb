from office365.runtime.client_value import ClientValue


class SPSiteCreationResponse(ClientValue):
    def __init__(self):
        super(SPSiteCreationResponse, self).__init__()
        self.SiteId = None
        self.SiteStatus = None
        self.SiteUrl = None

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.SPSiteCreationResponse"
