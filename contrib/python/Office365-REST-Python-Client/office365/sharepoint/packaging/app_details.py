from typing import Optional

from office365.sharepoint.entity import Entity


class AppDetails(Entity):
    """Specifies the detail properties for a SharePoint Add-in."""

    @property
    def eula_url(self):
        # type: () -> Optional[str]
        """Specifies the URL to get the End User License Agreement (EULA) information for the SharePoint Add-in."""
        return self.properties.get("EulaUrl", None)

    @property
    def privacy_url(self):
        # type: () -> Optional[str]
        """Specifies the URL of the privacy statement for the SharePoint Add-in."""
        return self.properties.get("PrivacyUrl", None)

    @property
    def publisher(self):
        # type: () -> Optional[str]
        """Specifies the publisher name for the SharePoint Add-in."""
        return self.properties.get("Publisher", None)

    @property
    def short_description(self):
        # type: () -> Optional[str]
        """Specifies a short description for the SharePoint Add-in."""
        return self.properties.get("ShortDescription", None)

    @property
    def support_url(self):
        # type: () -> Optional[str]
        """Specifies the URL of the support page for the SharePoint Add-in."""
        return self.properties.get("SupportUrl", None)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Packaging.AppDetails"
