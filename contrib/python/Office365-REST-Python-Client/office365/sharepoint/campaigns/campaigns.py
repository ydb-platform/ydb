from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class Campaigns(Entity):
    @staticmethod
    def get_campaign(context, campaign_id):
        """
        :type context: office365.sharepoint.client_context.ClientContext
        :param str campaign_id:  The campaign identifier.
        """
        return_type = ClientResult(context)
        payload = {"campaignId": campaign_id}
        qry = ServiceOperationQuery(
            Campaigns(context), "GetCampaign", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type
