from ad_api.base import Client, sp_endpoint, ApiResponse


class BidRecommendations(Client):
    """Amazon Advertising API for Sponsored Display

    Documentation: https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Bid%20Recommendations

    This API enables programmatic access for campaign creation, management, and reporting for Sponsored Display campaigns. For more information on the functionality, see the `Sponsored Display Support Center <https://advertising.amazon.com/help#GTPPHE6RAWC2C4LZ>`_ . For API onboarding information, see the `account setup <https://advertising.amazon.com/API/docs/en-us/setting-up/account-setup>`_  topic.

    This specification is available for download from the `Advertising API developer portal <https://d3a0d0y2hgofx6.cloudfront.net/openapi/en-us/sponsored-display/3-0/openapi.yaml>`_.

    """

    @sp_endpoint('/sd/targets/bid/recommendations', method='POST')
    def list_targets_bid_recommendations(self, **kwargs) -> ApiResponse:
        r"""
        Provides a list of bid recommendations based on the list of input advertised ASINs and targeting clauses in the same format as the targeting API. For each targeting clause in the request a corresponding bid recommendation will be returned in the response. Currently the API will accept up to 100 targeting clauses.
        The recommended bids are derrived from the last 7 days of winning auction bids for the related targeting clause.

        body: SDTargetingBidRecommendationsRequestV32 | REQUIRED {'description': 'A list of products to tailor bid recommendations for category and audience based targeting clauses.}'

            | '**products**': *SDGoalProduct*, {'description': 'A list of products for which to get targeting recommendations. minItems: 1, maxItems: 10000'}
            | '**bidOptimization**': *SDBidOptimizationV32 string*, {'description': 'Determines what the recommended bids will be optimized for. Note that "reach" for bid optimization is not currently supported. This note will be removed when these operations become available.', 'Enum': '[ clicks, conversions, reach ]'}
            | '**costType**': *SDCostTypeV31 string*, {'description': 'Determines what performance metric the bid recommendations will be optimized for. Note that "vcpm" for cost type is not currently supported. This note will be removed when these operations become available.', 'Enum': '[ cpc, vcpm ]'}
            | '**targetingClauses**': SDTargetingClauseV31

                type: object

                description: A list of targeting clauses to receive bid recommendations for.

                | '**expressionType**': *string*, {'description': 'Tactic T00020 ad groups only allow manual targeting.', 'Enum': '[ manual, auto ]'}
                | '**expression**': SDTargetingExpressionV31

                    type: object
                    description: The targeting expression to match against.

                    oneOf->

                    TargetingPredicate:

                        type: object
                        description: A predicate to match against in the Targeting Expression (only applicable to Product targeting - T00020).

                        * All IDs passed for category and brand-targeting predicates must be valid IDs in the Amazon Advertising browse system.
                        * Brand, price, and review predicates are optional and may only be specified if category is also specified.
                        * Review predicates accept numbers between 0 and 5 and are inclusive.
                        * When using either of the 'between' strings to construct a targeting expression the format of the string is 'double-double' where the first double must be smaller than the second double. Prices are not inclusive.

                        | '**type**': *string*, {'enum': '[ asinSameAs, asinCategorySameAs, asinBrandSameAs, asinPriceBetween, asinPriceGreaterThan, asinPriceLessThan, asinReviewRatingLessThan, asinReviewRatingGreaterThan, asinReviewRatingBetween, asinIsPrimeShippingEligible, asinAgeRangeSameAs, asinGenreSameAs ]'}
                        | '**value**': *string*, {'description': 'The value to be targeted. example: B0123456789'}


                    TargetingPredicateNested:

                        type: object
                        description: A behavioral event and list of targeting predicates that represents an Audience to target (only applicable to Audience targeting - T00030).

                        * For manual ASIN-grain targeting, the value array must contain only, 'exactProduct', 'similarProduct', 'releatedProduct' and 'lookback' TargetingPredicateBase components. The 'lookback' is mandatory and the value should be set to '7', '14', '30', '60', '90', '180' or '365'
                        * For manual Category-grain targeting, the value array must contain a 'lookback' and 'asinCategorySameAs' TargetingPredicateBase component, which can be further refined with optional brand, price, star-rating and shipping eligibility refinements. The 'lookback' is mandatory and the value should be set to '7', '14', '30', '60', '90', '180' or '365'
                        * For manual Category-grain targeting, the value array must contain a 'lookback' and 'asinCategorySameAs' TargetingPredicateBase component, which can be further refined with optional brand, price, star-rating and shipping eligibility refinements.
                        * For Amazon Audiences targeting, the TargetingPredicateNested type should be set to 'audience' and the value array should include one TargetingPredicateBase component with type set to 'audienceSameAs'.
                        * Future For manual Category-grain targeting, adding a 'negative' TargetingPredicateBase will exclude that TargetingPredicateNested from the overall audience.

                        | '**type**': *string*, {'enum': '[ views, audience, purchases ]'}
                        | '**value**': TargetingPredicateBase

                            type: object

                            description: A predicate to match against inside the TargetingPredicateNested component (only applicable to Audience targeting - T00030).

                            * All IDs passed for category and brand-targeting predicates must be valid IDs in the Amazon Advertising browse system.
                            * Brand, price, and review predicates are optional and may only be specified if category is also specified.
                            * Review predicates accept numbers between 0 and 5 and are inclusive.
                            * When using either of the 'between' strings to construct a targeting expression the format of the string is 'double-double' where the first double must be smaller than the second double. Prices are not inclusive.
                            * The exactProduct, similarProduct, and negative types do not utilize the value field.
                            * The only type currently applicable to Amazon Audiences targeting is 'audienceSameAs'.
                            * A 'relatedProduct' TargetingPredicateBase will Target an audience that has purchased a related product in the past 7,14,30,60,90,180, or 365 days.
                            * **Future** A 'negative' TargetingPredicateBase will exclude that TargetingPredicateNested from the overall audience.

                            | '**type**': *string*, {'enum': '[ asinCategorySameAs, asinBrandSameAs, asinPriceBetween, asinPriceGreaterThan, asinPriceLessThan, asinReviewRatingLessThan, asinReviewRatingGreaterThan, asinReviewRatingBetween, similarProduct, exactProduct, asinIsPrimeShippingEligible, asinAgeRangeSameAs, asinGenreSameAs, audienceSameAs, lookback ]'}
                            | '**value**': *string*, {'description': 'The value to be targeted. example: B0123456789'}

        Returns:

            ApiResponse

        """
        contentType = 'application/vnd.sdtargetingrecommendations.v3.2+json'
        headers = {'Content-Type': contentType}
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs, headers=headers)
