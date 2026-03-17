from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class CampaignOptimization(Client):
    """
    Specification: https://dtrnk0o2zy01c.cloudfront.net/openapi/en-us/dest/SponsoredProducts_prod_3p.json
    """

    @sp_endpoint('/sp/rules/campaignOptimization/eligibility', method='POST')
    def list_campaigns_optimization_eligibility(self, **kwargs) -> ApiResponse:
        r"""
        Gets a campaign optimization rule recommendation for SP campaigns.

        Request Body
            | SPCampaignOptimizationRecommendationsAPIRequest {
            | **campaignIds**\* (array) maxItems: 100. A list of campaign ids [
            |   RuleCampaignId (string): campaignId
            |   ]
            | }

        """
        contentType = 'application/vnd.optimizationrules.v1+json'
        headers = {'Content-Type': contentType}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/rules/campaignOptimization/{}', method='GET')
    def get_budget_campaign_optimization(self, campaignOptimizationId, **kwargs) -> ApiResponse:
        r"""
        Gets a campaign optimization rule specified by identifier.

        Param:
            | path **campaignOptimizationId**\* (string). The sp campaign optimization rule identifier.

        Returns:

            ApiResponse

        """

        return self._request(fill_query_params(kwargs.pop('path'), campaignOptimizationId), params=kwargs)

    @sp_endpoint('/sp/rules/campaignOptimization/{}', method='DELETE')
    def delete_budget_campaign_optimization(self, campaignOptimizationId, **kwargs) -> ApiResponse:
        r"""
        Deletes a campaign optimization rule specified by identifier.

        Param:
            | path **campaignOptimizationId**\* (string). The sp campaign optimization rule identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), campaignOptimizationId), params=kwargs)

    @sp_endpoint('/sp/rules/campaignOptimization', method='POST')
    def create_budget_campaign_optimization(self, **kwargs) -> ApiResponse:
        r"""
        Creates a campaign optimization rule.

        Request Body
            | CreateSPCampaignOptimizationRulesRequest {
            | **recurrence**\* RecurrenceType (string): The frequency of the rule application. Enum: ['DAILY']
            | **ruleAction**\* RuleAction (string): The action taken when the campaign optimization rule is enabled. Defaults to adopt Enum: ['ADOPT']
            | **ruleCondition**\* RuleConditionList [
            | maxItems: 3
            |   RuleCondition {
            |   **metricName**\* RuleConditionMetric (string): The advertising performance metric. ROAS is the only supported metric. Enum: ['ROAS', 'AVERAGE_BID']
            |   **comparisonOperator**\* ComparisonOperator (string): The comparison operator. Enum: ['GREATER_THAN', 'LESS_THAN', 'EQUAL_TO', 'LESS_THAN_OR_EQUAL_TO', 'GREATER_THAN_OR_EQUAL_TO']
            |   **threshold**\* number (double): The performance threshold value.
            |   }
            | ]
            | **ruleType**\* RuleType (string): The type of the campaign optimization rule. Only Support BID as of now Enum: ['BID', 'KEYWORD', 'PRODUCT']
            | **ruleName** RuleName (string): The campaign optimization rule name. maxLength: 355
            | **campaignIds**\* (array) maxItems: 20. A list of campaign ids [
            |   RuleCampaignId (string): campaignId
            |   ]


        Returns:

            ApiResponse

        """
        contentType = 'application/vnd.optimizationrules.v1+json'
        headers = {'Content-Type': contentType}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/rules/campaignOptimization', method='PUT')
    def edit_budget_campaign_optimization(self, **kwargs) -> ApiResponse:
        r"""
        Updates a campaign optimization rule.

        Request Body
            | UpdateSPCampaignOptimizationRulesRequest {
            | **recurrence**\* RecurrenceType (string): The frequency of the rule application. Enum: ['DAILY']
            | **ruleAction**\* RuleAction (string): The action taken when the campaign optimization rule is enabled. Defaults to adopt Enum: ['ADOPT']
            | **campaignOptimizationId**\* campaignOptimizationId (string): The persistent rule identifier. maxLength: 355
            | **ruleCondition**\* RuleConditionList [
            | maxItems: 3
            |   RuleCondition {
            |   **metricName**\* RuleConditionMetric (string): The advertising performance metric. ROAS is the only supported metric. Enum: ['ROAS', 'AVERAGE_BID']
            |   **comparisonOperator**\* ComparisonOperator (string): The comparison operator. Enum: ['GREATER_THAN', 'LESS_THAN', 'EQUAL_TO', 'LESS_THAN_OR_EQUAL_TO', 'GREATER_THAN_OR_EQUAL_TO']
            |   **threshold**\* number (double): The performance threshold value.
            |   }
            | ]
            | **ruleType**\* RuleType (string): The type of the campaign optimization rule. Only Support BID as of now Enum: ['BID', 'KEYWORD', 'PRODUCT']
            | **ruleName** RuleName (string): The campaign optimization rule name. maxLength: 355
            | **campaignIds**\* (array) maxItems: 20. A list of campaign ids [
            |   RuleCampaignId (string): campaignId
            |   ]


        Returns:

            ApiResponse

        """
        contentType = 'application/vnd.optimizationrules.v1+json'
        headers = {'Content-Type': contentType}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/rules/campaignOptimization/state', method='POST')
    def get_state_budget_campaign_optimization(self, **kwargs) -> ApiResponse:
        r"""
        Gets campaign optimization rule state. Recommended refresh frequency is once a day.

        Request Body
            | SPCampaignOptimizationNotificationAPIRequest {
            | **campaignIds**\* (array) maxItems: 100. A list of campaign ids [
            |   RuleCampaignId (string): campaignId
            |   ]
            | }

        Returns:

            ApiResponse

        """
        contentType = 'application/vnd.optimizationrules.v1+json'
        headers = {'Content-Type': contentType}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
