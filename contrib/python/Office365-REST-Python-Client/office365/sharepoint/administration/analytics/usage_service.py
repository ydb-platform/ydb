from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class SPAnalyticsUsageService(Entity):
    """Represents the entry point for the Event REST service exposed through CSOM"""

    def __init__(self, context):
        static_path = ResourcePath(
            "Microsoft.SharePoint.Administration.SPAnalyticsUsageService"
        )
        super(SPAnalyticsUsageService, self).__init__(context, static_path)

    def log_event(self, event_type_id, scope_id, item_id, site=None, user=None):
        """
        Used to log events.

        :param int event_type_id: Specifies the type of an analytics event
        :param str scope_id:
        :param str item_id:
        :param str site:
        :param str user:
        """
        payload = {
            "EventTypeId": event_type_id,
            "ItemId": item_id,
            "ScopeId": scope_id,
            "Site": site,
            "User": user,
        }
        qry = ServiceOperationQuery(self, "logevent", None, payload)
        self.context.add_query(qry)
        return self

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Administration.SPAnalyticsUsageService"
