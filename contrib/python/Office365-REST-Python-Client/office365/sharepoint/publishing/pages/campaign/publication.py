from typing import Optional

from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.publishing.highlights_info import HighlightsInfo
from office365.sharepoint.publishing.pages.page import SitePage


class CampaignPublication(SitePage):
    """ """

    def get_highlights_info(self):
        """ """
        return_type = HighlightsInfo(self.context)
        qry = ServiceOperationQuery(
            self, "GetHighlightsInfo", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def send_test_email(self):
        """ """
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "SendTestEmail", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def email_endpoint(self):
        # type: () -> Optional[str]
        """"""
        return self.properties.get("EmailEndpoint", None)

    @property
    def entity_type_name(self):
        return "SP.Publishing.CampaignPublication"
