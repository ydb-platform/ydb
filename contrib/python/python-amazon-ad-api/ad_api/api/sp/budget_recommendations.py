from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class BudgetRecommendations(Client):
    @sp_endpoint('/sp/campaigns/budgetRecommendations', method='POST')
    def list_campaigns_budget_recommendations(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        Get recommended daily budget and estimated missed opportunities for campaigns.

        Request Body (required)
            | BudgetRecommendationRequest {
            |       **campaignIds**\* (array) The campaign identifier. minItems: 1. maxItems: 100 [
            |           List of campaigns. (string)
            |   ]
            | }
        Returns
            ApiResponse
        """
        contentType = 'application/vnd.budgetrecommendation.v' + str(version) + '+json'
        headers = {'Content-Type': contentType}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
