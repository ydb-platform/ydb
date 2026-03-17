from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.portal.userprofiles.sharedwithme.document import (
    SharedWithMeDocument,
)


class DocumentsSharedWithGroup(Entity):
    """
    Provides methods for working with a list that shares documents with a SharePoint Group on the user's personal site.
    """

    @staticmethod
    def get_shared_with_group_docs(context, group_id=None):
        """
        Gets a shared documents for a group.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str group_id:
        """
        return_type = EntityCollection(context, SharedWithMeDocument)
        payload = {"groupId": group_id}
        qry = ServiceOperationQuery(
            DocumentsSharedWithGroup(context),
            "GetSharedWithGroupDocs",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.UserProfiles.DocumentsSharedWithGroup"
