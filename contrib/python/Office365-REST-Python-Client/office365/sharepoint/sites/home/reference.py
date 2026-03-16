from office365.runtime.client_value import ClientValue


class SPHSiteReference(ClientValue):
    """"""

    def __init__(self, logo_url=None, title=None, url=None):
        self.LogoUrl = logo_url
        self.Title = title
        self.Url = url

    @property
    def entity_type_name(self):
        return "SP.SPHSiteReference"
