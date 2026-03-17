from office365.runtime.client_value import ClientValue


class GroupSiteInfo(ClientValue):
    def __init__(self, site_url=None, site_status=None):
        """
        :param str site_url: Site url
        :param int site_status: Site status
        """
        super(GroupSiteInfo, self).__init__()
        self.SiteStatus = site_status
        self.SiteUrl = site_url
        self.DocumentsUrl = None
        self.ErrorMessage = None
        self.GroupId = None
