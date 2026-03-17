from office365.runtime.client_value import ClientValue


class SiteInfoForSitePicker(ClientValue):
    def __init__(self, Error=None, site_id=None, site_name=None, Url=None):
        # type: (str, str, str, str) -> None
        self.Error = Error
        self.SiteId = site_id
        self.SiteName = site_name
        self.Url = Url

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.SiteInfoForSitePicker"
