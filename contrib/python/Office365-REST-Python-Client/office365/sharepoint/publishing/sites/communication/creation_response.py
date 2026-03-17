from office365.runtime.client_value import ClientValue


class CommunicationSiteCreationResponse(ClientValue):
    def __init__(self, site_status=None, site_url=None):
        self.SiteStatus = site_status
        self.SiteUrl = site_url
