from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class AnalyticsUsageEntry(Entity):
    """Specifies an analytics usage entry to log user or system events"""

    @staticmethod
    def log_analytics_event(context, event_type_id, item_id):
        """
        Creates and logs an analytics event into the analytics pipeline.

        :type context: office365.sharepoint.client_context.ClientContext
        :param str event_type_id: The event type identifier of the analytics event to create.
        :param str item_id: The identifier of the item for which the event is being logged.
        """
        payload = {
            "eventTypeId": event_type_id,
            "itemId": item_id,
        }
        binding_type = AnalyticsUsageEntry(context)
        qry = ServiceOperationQuery(
            binding_type, "LogAnalyticsEvent", None, payload, is_static=True
        )
        context.add_query(qry)
        return binding_type

    @staticmethod
    def log_analytics_app_event2(
        context, app_event_type_id, item_id, rollup_scope_id, site_id, user_id
    ):
        """
        Creates and logs an analytics event into the analytics pipeline with additional parameters.

        :type context: office365.sharepoint.client_context.ClientContext
        :param str app_event_type_id: The event type identifier of the analytics event to be created.
        :param str item_id: The identifier of the item for which the event is being logged.
        :param str rollup_scope_id: The identifier of a rollup scope. Events for different items with the same rollup
            scope can be aggregated together at that rollup scope in addition to being counted just at the item scope.
        :param str site_id: The identifier of the item's site.
        :param str user_id: The identifier of the user generating the event.
        """
        payload = {
            "appEventTypeId": app_event_type_id,
            "itemId": item_id,
            "rollupScopeId": rollup_scope_id,
            "site_id": site_id,
            "userId": user_id,
        }
        binding_type = AnalyticsUsageEntry(context)
        qry = ServiceOperationQuery(
            binding_type, "LogAnalyticsAppEvent2", None, payload
        )
        qry.static = True
        context.add_query(qry)
        return binding_type

    @property
    def entity_type_name(self):
        return "SP.Analytics.AnalyticsUsageEntry"
