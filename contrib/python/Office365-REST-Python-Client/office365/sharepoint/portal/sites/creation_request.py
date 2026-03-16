from office365.runtime.client_value import ClientValue


class SPSiteCreationRequest(ClientValue):
    def __init__(
        self, title, url, owner=None, lcid=1033, web_template="SITEPAGEPUBLISHING#0"
    ):
        """
        :param str title:
        :param str url:
        :param str owner:
        :param int lcid:
        :param str web_template:
        """
        super(SPSiteCreationRequest, self).__init__()
        self.Title = title
        self.Url = url
        self.WebTemplate = web_template
        self.Owner = owner
        self.Lcid = lcid
        self.ShareByEmailEnabled = False
        self.Classification = ""
        self.Description = ""
        self.SiteDesignId = "00000000-0000-0000-0000-000000000000"
        self.HubSiteId = "00000000-0000-0000-0000-000000000000"
        self.WebTemplateExtensionId = "00000000-0000-0000-0000-000000000000"

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.SPSiteCreationRequest"
