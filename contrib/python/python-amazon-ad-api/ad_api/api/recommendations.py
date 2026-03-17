from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class Recommendations(Client):
    """ """

    @sp_endpoint('/recommendations/list', method='POST')
    def list_recommendations(self, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        Retrieves a list of recommendations.

        Request Body (required)
            | ListRecommendationsRequest {
            | **maxResults** (integer) default: 500 minimum: 1 maximum: 500
            | **nextToken** (string) Token to retrieve the next page of results.
            | **filters**\* (array) minItems: 1 maxItems: 10 [
            |   ListRecommendationsFilter {
            |       **include** (boolean): Flag to specify if the filter should be included or excluded. default: true
            |       **field**\* FilterField (string): Field to filter by. Enum: ['RECOMMENDATION_ID', 'AD_PRODUCT', 'RECOMMENDATION_TYPE', 'STATUS', 'GROUPING_TYPE', 'CAMPAIGN_ID']
            |       **values**\* (array) minItems: 1 maxItems: 500 [
            |           string
            |       ]
            |       **operator** FilterOperator (string): Operator to filter field by. Enum: ['EXACT']
            |       }
            |   ]
            | }
        Returns
            ApiResponse
        """
        schema_version = 'application/vnd.listRecommendationsRequest.v' + str(version) + '+json'
        headers = {"Accept": schema_version, "Content-Type": schema_version}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/recommendations/apply', method='POST')
    def apply_recommendations(self, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        Applies one or more recommendations.

        Request Body (required)
            | ApplyRecommendationsRequest {
            | **recommendationIds**\* (array) minItems: 1 maxItems: 100 [
            |       (string) Recommendation identifier.
            |   ]
            | }
        Returns
            ApiResponse
        """
        schema_version = 'application/vnd.applyRecommendationsRequest.v' + str(version) + '+json'
        headers = {"Accept": schema_version, "Content-Type": schema_version}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/recommendations/{}', method='PUT')
    def update_recommendation(self, recommendationId, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        Updates a recommendation.

        path **recommendationId**:*string* | Required. The identifier of the recommendation.

        Request Body (required)
            | UpdateRecommendationRequest {
            | **ruleBasedBidding** UpdateRuleBasedBidding {
            |       Can only be updated for recommendations with recommendationType NEW_CAMPAIGN_BIDDING_RULE or CAMPAIGN_BIDDING_RULE.
            |       **recommendedRuleRoas**\* (number)
            |   }
            | **recommendedValue** (string) Recommended value of the recommendation. Type of data expected for each recommendation type:
            | **budgetRule** UpdateBudgetRule {
            |       **ruleDetails**\* UpdateBudgetRuleDetails {
            |           **duration** UpdateBudgetRuleDuration {
            |               **dateRangeTypeDuration** UpdateBudgetRuleDurationDateRange {
            |                   **endDate** (string): End date of the budget rule in YYYY-MM-DD format. The end date is inclusive.
            |           }
            |           **budgetIncreaseBy** BudgetRuleIncreaseBy {
            |               **value**\*(number): Budget of the rule.
            |           }
            |           **ruleName**\* (string): Name of the budget rule. Required to be unique within a campaign. minLength: 1 maxLength: 355
            |           **performanceMeasureCondition** BudgetRulePerformanceMeasureCondition {
            |               **threshold**\*(number): Threshold of the performance metric.
            |           }
            |       }
            |   }
            | }
        Returns
            ApiResponse


        """
        schema_version = 'application/vnd.updateRecommendationRequest.v' + str(version) + '+json'
        headers = {"Accept": schema_version, "Content-Type": schema_version}
        return self._request(
            fill_query_params(kwargs.pop('path'), recommendationId),
            data=Utils.convert_body(kwargs.pop('body'), False),
            params=kwargs,
            headers=headers,
        )
