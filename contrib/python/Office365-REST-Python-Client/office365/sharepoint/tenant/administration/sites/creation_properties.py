from office365.runtime.client_value import ClientValue


class SiteCreationProperties(ClientValue):
    def __init__(
        self,
        title=None,
        url=None,
        owner=None,
        owner_name=None,
        template=None,
        site_uni_name=None,
    ):
        """Sets the initial properties for a new site when it is created.

        :param str owner: Gets or sets the login name of the owner of the new site
        :param str url: Gets or sets the new site’s URL.
        :param str template: Gets or sets the web template name of the new site.
        :param str site_uni_name:
        """
        super(SiteCreationProperties, self).__init__()
        self.Url = url
        self.Owner = owner
        self.OwnerName = owner_name
        self.Title = title
        self.Template = template
        self.SiteUniName = site_uni_name

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.SiteCreationProperties"
