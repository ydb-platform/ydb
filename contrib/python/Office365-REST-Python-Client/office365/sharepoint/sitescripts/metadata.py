from office365.runtime.client_value import ClientValue


class SiteScriptMetadata(ClientValue):
    """Represents metadata about a SharePoint site script in the SharePoint framework.
    Site scripts are used to automate the provisioning of SharePoint sites by defining actions like
    applying a theme, adding lists, and configuring site settings."""

    def __init__(
        self,
        id_=None,
        content=None,
        description=None,
        is_site_script_package=None,
        title=None,
        version=None,
    ):
        """
        :param str id_: unique identifier (GUID) for the site script. This is used to uniquely identify the script
             within SharePoint.
        :param str content: The actual JSON content of the site script. This contains the actions that the script
             will execute when applied to a SharePoint site.
        :param str description:
        :param bool is_site_script_package:
        :param str title:
        :param int version:
        """
        self.Id = id_
        self.Content = content
        self.Description = description
        self.IsSiteScriptPackage = is_site_script_package
        self.Title = title
        self.Version = version

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Utilities.WebTemplateExtensions.SiteScriptMetadata"
