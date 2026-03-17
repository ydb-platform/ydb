from typing import TYPE_CHECKING

from office365.runtime.client_result import ClientResult
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.tenant.administration.datagovernance.client_base import (
    SPDataGovernanceRestApiClientBase,
)
from office365.sharepoint.tenant.administration.datagovernance.insight_metadata import (
    SPDataGovernanceInsightMetadata,
)

if TYPE_CHECKING:
    from office365.sharepoint.client_context import ClientContext


class SPDataGovernanceInsightRestApiClient(SPDataGovernanceRestApiClientBase):
    """"""

    def __init__(self, context, authorization_header, url, user_agent):
        # type: (ClientContext, str, str, str) -> None
        static_path = ServiceOperationPath(
            "Microsoft.Online.SharePoint.TenantAdministration.SPDataGovernanceInsightRestApiClient",
            {
                "authorizationHeader": authorization_header,
                "url": url,
                "userAgent": user_agent,
            },
        )
        super(SPDataGovernanceInsightRestApiClient, self).__init__(context, static_path)

    def create_data_access_governance_report(
        self,
        report_entity,
        workload,
        report_type,
        file_sensitivity_label_name,
        file_sensitivity_label_guid,
        name,
        template,
        privacy,
        site_sensitivity_label_guid,
        count_of_users_more_than,
    ):
        """ """
        return_type = ClientResult(self.context, SPDataGovernanceInsightMetadata())
        payload = {
            "reportEntity": report_entity,
            "workload": workload,
            "reportType": report_type,
            "fileSensitivityLabelName": file_sensitivity_label_name,
            "fileSensitivityLabelGUID": file_sensitivity_label_guid,
            "name": name,
            "template": template,
            "privacy": privacy,
            "siteSensitivityLabelGUID": site_sensitivity_label_guid,
            "countOfUsersMoreThan": count_of_users_more_than,
        }
        qry = ServiceOperationQuery(
            self,
            "CreateDataAccessGovernanceReport",
            None,
            payload,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def export_spo_data_access_governance_insight(self, report_id):
        return_type = ClientResult(self.context, str())
        payload = {"reportId": report_id}
        qry = ServiceOperationQuery(
            self,
            "ExportSPODataAccessGovernanceInsight",
            None,
            payload,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type
