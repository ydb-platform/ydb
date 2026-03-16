from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.portal.userprofiles.sharedwithme.document import (
    SharedWithMeDocument,
)


class SharedWithMeItems(Entity):
    """"""

    @staticmethod
    def shared_with_me(context):
        """
        :param office365.sharepoint.client_context.ClientContext context: Client context
        """
        binding_type = SharedWithMeItems(context)
        return_type = EntityCollection(context, SharedWithMeDocument)
        qry = ServiceOperationQuery(
            binding_type, "SharedWithMe", return_type=return_type, is_static=True
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.UserProfiles.SharedWithMeItems"
