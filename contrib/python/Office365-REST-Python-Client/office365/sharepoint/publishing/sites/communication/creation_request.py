from office365.runtime.client_value import ClientValue


class CommunicationSiteCreationRequest(ClientValue):
    def __init__(
        self,
        title,
        url,
        description=None,
        lcid=None,
        classification=None,
        allow_filesharing_for_guest_users=None,
        web_template_extension_id=None,
        site_design_id=None,
    ):
        """
        Options for configuring the Communication Site that will be created.

        :param str title: Site title
        :param str url: Absolute site url
        :param str description:
        :param str lcid: The LCID (locale identifier) for a site
        """
        self.Title = title
        self.Url = url
        self.Description = description
        self.lcid = lcid
        self.Classification = classification
        self.AllowFileSharingForGuestUsers = allow_filesharing_for_guest_users
        self.WebTemplateExtensionId = web_template_extension_id
        self.SiteDesignId = site_design_id

    @property
    def entity_type_name(self):
        return "SP.Publishing.CommunicationSiteCreationRequest"
