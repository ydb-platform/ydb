from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class BudgetRulesRecommendations(Client):
    @sp_endpoint('/sp/campaigns/budgetRules/recommendations', method='POST')
    def list_campaigns_budget_rules_recommendations(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        Gets a list of special events with suggested date range and suggested budget increase for a campaign specified by identifier.

        Request Body (required)
            | SPBudgetRulesRecommendationEventRequest {
            |       **campaignId**\* (string) The campaign identifier.
            | }
        Returns
            ApiResponse
        """
        contentType = 'application/vnd.spbudgetrulesrecommendation.v' + str(version) + '+json'
        headers = {'Content-Type': contentType, "Accept": contentType}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
