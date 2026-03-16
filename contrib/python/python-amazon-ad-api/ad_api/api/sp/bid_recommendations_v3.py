from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class BidRecommendationsV3(Client):
    @sp_endpoint('/sp/targets/bid/recommendations', method='POST')
    def get_bid_recommendations(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        Gets theme-based bid recommendations for new or existing ad groups.

        Request Body (oneOf)
            | AdGroupThemeBasedBidRecommendationRequest {
            | **targetingExpressions**\* TargetingExpressionList [
            | example: [OrderedMap {'type': 'CLOSE_MATCH'}, OrderedMap {'type': 'LOOSE_MATCH'}, OrderedMap {'type': 'SUBSTITUTES'}, OrderedMap {'type': 'COMPLEMENTS'}]
            | maxItems: 100. The list of targeting expressions. Maximum of 100 per request, use pagination for more if needed.
            |   TargetingExpression {
            |   The targeting expression. The `type` property specifies the targeting option. Use `CLOSE_MATCH` to match your auto targeting ads closely to the specified value. Use `LOOSE_MATCH` to match your auto targeting ads broadly to the specified value. Use `SUBSTITUTES` to display your auto targeting ads along with substitutable products. Use `COMPLEMENTS` to display your auto targeting ads along with affiliated products. Use `KEYWORD_BROAD_MATCH` to broadly match your keyword targeting ads with search queries. Use `KEYWORD_EXACT_MATCH` to exactly match your keyword targeting ads with search queries. Use `KEYWORD_PHRASE_MATCH` to match your keyword targeting ads with search phrases.
            |   example: {'type': 'CLOSE_MATCH'}
            |   **type**\*(string) Enum: ['CLOSE_MATCH', 'LOOSE_MATCH', 'SUBSTITUTES', 'COMPLEMENTS', 'KEYWORD_BROAD_MATCH', 'KEYWORD_EXACT_MATCH', 'KEYWORD_PHRASE_MATCH']
            |   value(string): The targeting expression value.
            |   }
            | ]
            | **campaignId**\*(string): The campaign identifier.
            | **recommendationType**\*(string): The bid recommendation type. Enum: ['BIDS_FOR_EXISTING_AD_GROUP']
            | **adGroupId**\*(string): The ad group identifier.
            | }

            | AsinsThemeBasedBidRecommendationRequest {
            | **asins**\*(array): maxItems: 50. The list of ad ASINs in the ad group.
            | **targetingExpressions**\* TargetingExpressionList [
            | example: [OrderedMap {'type': 'CLOSE_MATCH'}, OrderedMap {'type': 'LOOSE_MATCH'}, OrderedMap {'type': 'SUBSTITUTES'}, OrderedMap {'type': 'COMPLEMENTS'}]
            | maxItems: 100. The list of targeting expressions. Maximum of 100 per request, use pagination for more if needed.
            |   TargetingExpression {
            |   The targeting expression. The `type` property specifies the targeting option. Use `CLOSE_MATCH` to match your auto targeting ads closely to the specified value. Use `LOOSE_MATCH` to match your auto targeting ads broadly to the specified value. Use `SUBSTITUTES` to display your auto targeting ads along with substitutable products. Use `COMPLEMENTS` to display your auto targeting ads along with affiliated products. Use `KEYWORD_BROAD_MATCH` to broadly match your keyword targeting ads with search queries. Use `KEYWORD_EXACT_MATCH` to exactly match your keyword targeting ads with search queries. Use `KEYWORD_PHRASE_MATCH` to match your keyword targeting ads with search phrases.
            |   example: {'type': 'CLOSE_MATCH'}
            |   **type**\*(string) Enum: ['CLOSE_MATCH', 'LOOSE_MATCH', 'SUBSTITUTES', 'COMPLEMENTS', 'KEYWORD_BROAD_MATCH', 'KEYWORD_EXACT_MATCH', 'KEYWORD_PHRASE_MATCH']
            |   value(string): The targeting expression value.
            |   }
            | ]
            | **bidding**\* Bidding control configuration for the campaign.
            |   **adjustments** (array) maxItems: 2. Placement adjustment configuration for the campaign.
            |       PlacementAdjustment {
            |       Specifies bid adjustments based on the placement location. Use `PLACEMENT_TOP` for the top of the search page. Use `PLACEMENT_PRODUCT_PAGE` for a product page.
            |           example: {'predicate': 'PLACEMENT_TOP', 'percentage': '100'}
            |           **predicate** (string) Enum: ['PLACEMENT_TOP', 'PLACEMENT_PRODUCT_PAGE']
            |           **percentage** (integer) maximum: 900 minimum: 0
            |       }

            |   **strategy**\* BiddingStrategy (string): The bidding strategy selected for the campaign. Use `LEGACY_FOR_SALES` to lower your bid in real time when your ad may be less likely to convert to a sale. Use `AUTO_FOR_SALES` to increase your bid in real time when your ad may be more likely to convert to a sale or lower your bid when less likely to convert to a sale. Use `MANUAL` to use your exact bid along with any manual adjustments. Enum: ['LEGACY_FOR_SALES', 'AUTO_FOR_SALES', 'MANUAL', 'RULE_BASED']
            | **recommendationType**\*(string): The bid recommendation type. Enum: ['BIDS_FOR_EXISTING_AD_GROUP']
            | }
        Returns
            ApiResponse
        """

        json_version = 'application/vnd.spthemebasedbidrecommendation.v' + str(version) + "+json"

        headers = {"Content-Type": json_version}

        return self._request(fill_query_params(kwargs.pop('path')), data=kwargs.pop('body'), params=kwargs, headers=headers)
