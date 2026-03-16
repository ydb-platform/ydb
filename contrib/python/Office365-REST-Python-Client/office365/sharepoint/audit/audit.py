from typing import Optional

from office365.sharepoint.entity import Entity


class Audit(Entity):
    """
    Enables auditing of how site collections, sites, lists, folders, and list items are accessed, changed, and used.
    """

    @property
    def allow_designer(self):
        # type: () -> Optional[bool]
        """
        Specifies whether a designer can be used on this site collection.
        See Microsoft.SharePoint.Client.Web.AllowDesignerForCurrentUser, which is the scalar property used
        to determine the behavior for the current user. The default, if not disabled on the Web application, is "true".
        """
        return self.properties.get("AllowDesigner", None)
