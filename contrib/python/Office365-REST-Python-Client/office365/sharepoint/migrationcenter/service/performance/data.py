from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.migrationcenter.service.performance.dashboard_data import (
    PerformanceDashboardData,
)
from office365.sharepoint.migrationcenter.service.performance.entity_data import (
    MigrationPerformanceEntityData,
)


class PerformanceData(MigrationPerformanceEntityData):
    """"""

    def __init__(self, context, resource_path=None):
        if resource_path is None:
            resource_path = ResourcePath(
                "Microsoft.Online.SharePoint.MigrationCenter.Service.PerformanceData"
            )
        super(PerformanceData, self).__init__(context, resource_path)

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.MigrationCenter.Service.PerformanceData"


class PerformanceDataCollection(EntityCollection[PerformanceData]):
    def __init__(self, context, resource_path=None):
        super(PerformanceDataCollection, self).__init__(
            context, PerformanceData, resource_path
        )

    def get_data(self, start_time=None, end_time=None, agent_id=None, time_unit=None):
        """ """
        return_type = ClientResult(self.context, PerformanceDashboardData())
        payload = {
            "StartTime": start_time,
            "EndTime": end_time,
            "AgentId": agent_id,
            "TimeUnit": time_unit,
        }
        qry = ServiceOperationQuery(self, "GetData", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type
