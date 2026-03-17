from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.tenant.administration.copilot.agentinsights.report_metadata import (
    SPOCopilotAgentInsightsReportMetadata,
)
from office365.sharepoint.tenant.administration.datagovernance.client_base import (
    SPDataGovernanceRestApiClientBase,
)


class SPOCopilotAgentInsightsRestApiClient(SPDataGovernanceRestApiClientBase):
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
        super(SPOCopilotAgentInsightsRestApiClient, self).__init__(context, static_path)

    def get_all_copilot_agent_insights_reports_metadata(self):
        return_type = ClientResult(
            self.context, ClientValueCollection(SPOCopilotAgentInsightsReportMetadata)
        )
        qry = ServiceOperationQuery(
            self,
            "GetAllCopilotAgentInsightsReportsMetadata",
            None,
            None,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type
