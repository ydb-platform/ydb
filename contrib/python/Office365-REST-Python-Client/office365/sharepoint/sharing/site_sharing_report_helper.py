from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.entity import Entity
from office365.sharepoint.sharing.reports.site_capabilities import (
    SiteSharingReportCapabilities,
)
from office365.sharepoint.sharing.site_sharing_report_status import (
    SiteSharingReportStatus,
)


class SiteSharingReportHelper(Entity):
    @staticmethod
    def cancel_sharing_report_job(context):
        # type: (ClientContext) -> ClientResult[SiteSharingReportStatus]
        return_type = ClientResult(context, SiteSharingReportStatus())
        binding_type = SiteSharingReportHelper(context)
        qry = ServiceOperationQuery(
            binding_type, "CancelSharingReportJob", None, None, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def create_sharing_report_job(context, web_url, folder_url):
        """
        :type context: office365.sharepoint.client_context.ClientContext
        :param str web_url:
        :param str folder_url:
        """
        return_type = ClientResult(context, SiteSharingReportStatus())
        payload = {"webUrl": web_url, "folderUrl": folder_url}
        binding_type = SiteSharingReportHelper(context)
        qry = ServiceOperationQuery(
            binding_type,
            "CreateSharingReportJob",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_site_sharing_report_capabilities(context):
        """
        :type context: office365.sharepoint.client_context.ClientContext
        """
        return_type = ClientResult(context, SiteSharingReportCapabilities())
        binding_type = SiteSharingReportHelper(context)
        qry = ServiceOperationQuery(
            binding_type,
            "GetSiteSharingReportCapabilities",
            None,
            None,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.Sharing.SiteSharingReportHelper"
