
from typing import Any

from ad_api.base import ApiResponse, Client, Utils, fill_query_params, sp_endpoint


class ScheduleBasedBidOptimizationRules(Client):
    """
    A class representing the Amazon Advertising API endpoint for Schedule Based Bid Optimization Rules.
    https://advertising.amazon.com/API/docs/en-us/sponsored-products/3-0/openapi/prod#tag/Optimization-Rules

    Methods
    -------
    create_optimization_rules(**kwargs)
        Creates a new optimization rule.
    update_optimization_rules(**kwargs)
        Updates an existing optimization rule.
    associate_optimization_rules_with_campaign(campaignId, **kwargs)
        Associates an optimization rule with a campaign.
    searches_optimization_rules(**kwargs)
        Searches for optimization rules that match the specified criteria.
    """

    HEADERS = {
        "Content-Type": "application/vnd.spoptimizationrules.v1+json",
        "Accept": "application/vnd.spoptimizationrules.v1+json",
    }

    @sp_endpoint('/sp/rules/optimization', method='POST')
    def create_optimization_rules(self, **kwargs) -> ApiResponse:
        return self._request(
            str(kwargs.pop('path')),
            data=Utils.convert_body(kwargs.pop('body'), False),
            params=kwargs,
            headers=self.HEADERS,
        )

    @sp_endpoint('/sp/rules/optimization', method='PUT')
    def update_optimization_rules(self, **kwargs) -> ApiResponse:
        return self._request(
            str(kwargs.pop('path')),
            data=Utils.convert_body(kwargs.pop('body'), False),
            params=kwargs,
            headers=self.HEADERS,
        )

    @sp_endpoint('/sp/campaigns/{}/optimizationRules', method='POST')
    def associate_optimization_rules_with_campaign(self, campaignId: str, **kwargs) -> ApiResponse:
        return self._request(
            fill_query_params(kwargs.pop('path'), campaignId),
            data=Utils.convert_body(kwargs.pop('body'), False),
            params=kwargs,
            headers=self.HEADERS,
        )

    @sp_endpoint('/sp/rules/optimization/search', method='POST')
    def searches_optimization_rules(self, **kwargs) -> ApiResponse:
        return self._request(
            str(kwargs.pop('path')),
            data=Utils.convert_body(kwargs.pop('body'), False),
            params=kwargs,
            headers=self.HEADERS,
        )
