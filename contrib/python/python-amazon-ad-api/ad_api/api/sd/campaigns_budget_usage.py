from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class CampaignsBudgetUsage(Client):
    @sp_endpoint('/sd/campaigns/budget/usage', method='POST')
    def list_campaigns_budget_usage(self, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        Budget usage API for SD campaigns

        Request Body
            | BudgetUsageCampaignRequest {
            | **campaignIds**\* (array) maxItems: 100 [
            |   A list of campaign IDs (string)
            |   ]
            | }
        Returns
            ApiResponse
        """
        schema_version = 'application/vnd.sdcampaignbudgetusage.v' + str(version) + '+json'
        headers = {"Accept": schema_version, "Content-Type": schema_version}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
