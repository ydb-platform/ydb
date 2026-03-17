from typing import Optional

from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class MySiteLinks(Entity):
    """The MySiteLinks object provides links for a user’s personal site."""

    @staticmethod
    def get_my_site_links(context):
        """
        Return Type: Microsoft.SharePoint.Portal.UserProfiles.MySiteLinks
        The GetMySiteLinks function retrieves a MySiteLinks (section 3.1.5.22) object for the current user.

        :param office365.sharepoint.client_context.ClientContext context: Client context
        """
        return_type = MySiteLinks(context)
        qry = ServiceOperationQuery(
            MySiteLinks(context), "GetMySiteLinks", None, None, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @property
    def all_documents_link(self):
        # type: () -> Optional[str]
        """
        This property value is the URL of the user’s document library on their personal site. This property value is
        null if the user does not have a personal site or the user does not have a document library in their personal
        site.
        """
        return self.properties.get("AllDocumentsLink", None)

    @property
    def all_sites_link(self):
        # type: () -> Optional[str]
        """
        This property value is the URL of the user’s followed sites view on their personal site.  This property value
        is null if the user does not have a personal site or social features are not enabled on their personal site.
        """
        return self.properties.get("AllSitesLink", None)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.UserProfiles.MySiteLinks"
