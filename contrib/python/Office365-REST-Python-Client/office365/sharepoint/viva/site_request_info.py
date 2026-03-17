from office365.runtime.client_value import ClientValue


class VivaSiteRequestInfo(ClientValue):
    def __init__(self, is_already_added=None, site_url=None):
        """
        :param bool is_already_added:
        :param str site_url:
        """
        self.IsAlreadyAdded = is_already_added
        self.SiteUrl = site_url

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.VivaSiteRequestInfo"
