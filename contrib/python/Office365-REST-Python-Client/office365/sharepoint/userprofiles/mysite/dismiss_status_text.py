from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class MySiteDismissStatusText(Entity):
    """Provides methods to dismiss status text for the personal online document library page."""

    @staticmethod
    def dismiss_status_text(context):
        """
        Dismiss the status text for the personal online document library page.
        :param office365.sharepoint.client_context.ClientContext context: Client context
        """
        binding_type = MySiteDismissStatusText(context)
        qry = ServiceOperationQuery(binding_type, "DismissStatusText", is_static=True)
        context.add_query(qry)
        return binding_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.UserProfiles.MySiteDismissStatusText"
