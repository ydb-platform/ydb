from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.viva.app_configuration import AppConfiguration
from office365.sharepoint.viva.connections_page import VivaConnectionsPage
from office365.sharepoint.viva.dashboard_configuration import DashboardConfiguration
from office365.sharepoint.viva.home import VivaHome


class EmployeeEngagement(Entity):
    def __init__(self, context):
        super(EmployeeEngagement, self).__init__(
            context, ResourcePath("SP.EmployeeEngagement")
        )

    def dashboard_content(self, override_language_code=None):
        """
        :param str override_language_code:
        """
        return_type = ClientResult(self.context, str())
        payload = {"override_language_code": override_language_code}
        qry = ServiceOperationQuery(
            self, "DashboardContent", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def full_dashboard_content(
        self, canvas_as_json=None, include_personalization_data=None
    ):
        """
        :param bool canvas_as_json:
        :param bool include_personalization_data:
        """
        return_type = ClientResult(self.context, DashboardConfiguration())
        payload = {
            "canvasAsJson": canvas_as_json,
            "includePersonalizationData": include_personalization_data,
        }
        qry = ServiceOperationQuery(
            self, "FullDashboardContent", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def viva_home_configuration(self):
        return_type = ClientResult(self.context, {})
        qry = FunctionQuery(self, "VivaHomeConfiguration", None, return_type)
        self.context.add_query(qry)
        return return_type

    def viva_home(self):
        return_type = VivaHome(self.context)
        qry = ServiceOperationQuery(self, "VivaHome", return_type=return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def app_configuration(self):
        return self.properties.get(
            "AppConfiguration",
            AppConfiguration(
                self.context, ResourcePath("AppConfiguration", self.resource_path)
            ),
        )

    @property
    def viva_connections_page(self):
        return self.properties.get(
            "VivaConnectionsPage",
            VivaConnectionsPage(
                self.context, ResourcePath("VivaConnectionsPage", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "AppConfiguration": self.app_configuration,
                "VivaConnectionsPage": self.viva_connections_page,
            }
            default_value = property_mapping.get(name, None)
        return super(EmployeeEngagement, self).get_property(name, default_value)
