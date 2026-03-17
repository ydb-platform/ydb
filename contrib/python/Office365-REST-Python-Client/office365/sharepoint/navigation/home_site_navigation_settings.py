from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class HomeSiteNavigationSettings(Entity):
    def __init__(self, context, resource_path=None):
        if resource_path is None:
            resource_path = ResourcePath(
                "Microsoft.SharePoint.Navigation.REST.HomeSiteNavigationSettings"
            )
        super(HomeSiteNavigationSettings, self).__init__(context, resource_path)

    def enable_global_navigation(self, is_enabled):
        """
        :param bool is_enabled:
        """
        payload = {"isEnabled": is_enabled}
        qry = ServiceOperationQuery(self, "EnableGlobalNavigation", None, payload)
        self.context.add_query(qry)
        return self

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Navigation.REST.HomeSiteNavigationSettings"
