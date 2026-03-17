from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.tenant.insights.report_metadata import (
    SPTenantIBInsightsReportMetadata,
)


class SPTenantIBInsightsReportManager(Entity):
    """ """

    def __init__(self, context):
        static_path = ResourcePath(
            "Microsoft.SharePoint.Insights.SPTenantIBInsightsReportManager"
        )
        super(SPTenantIBInsightsReportManager, self).__init__(context, static_path)

    def create_report(self):
        """"""
        return_type = SPTenantIBInsightsReportMetadata(self.context)
        qry = ServiceOperationQuery(
            self,
            "CreateReport",
            None,
            None,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Insights.SPTenantIBInsightsReportManager"
