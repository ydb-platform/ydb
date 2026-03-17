from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class BidRecommendations(Client):
    @sp_endpoint('/v2/sp/adGroups/{}/bidRecommendations', method='GET')
    def get_ad_group_bid_recommendations(self, adGroupId, **kwargs) -> ApiResponse:
        r"""
        get_ad_group_bid_recommendations(self, adGroupId, \*\*kwargs) -> ApiResponse

        Gets a bid recommendation for an ad group.

            path **adGroupId**:*number* | Required. The identifier of an existing ad group.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), adGroupId), params=kwargs)

    @sp_endpoint('/v2/sp/keywords/{}/bidRecommendations', method='GET')
    def get_keyword_bid_recommendations(self, keywordId, **kwargs) -> ApiResponse:
        r"""
        get_ad_group_bid_recommendations(self, adGroupId, \*\*kwargs) -> ApiResponse

        Gets a bid recommendation for a keyword.

            path **keywordId**:*number* | Required. The identifier of an existing keyword.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), keywordId), params=kwargs)

    @sp_endpoint('/v2/sp/keywords/bidRecommendations', method='POST')
    def get_keywords_bid_recommendations(self, **kwargs) -> ApiResponse:
        r"""
        get_keywords_bid_recommendations(self, \*\*kwargs) -> ApiResponse:

        Gets bid recommendations for keywords.

        body: | REQUIRED {'description': 'An array of ad groups.}'

            | '**adGroupId**': *number*, {'description': 'The identifier of the ad group.'}
            | keywords {
            | '**keywords**': *string*, {'description': 'The keyword text.'}
            | '**matchType**': *string*, {'description': 'The type of match', 'Enum': '[ exact, phrase, broad ]'}
            | }


        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/v2/sp/targets/bidRecommendations', method='POST')
    def get_targets_bid_recommendations(self, **kwargs) -> ApiResponse:
        r"""
        get_targets_bid_recommendations(self, \*\*kwargs) -> ApiResponse:

        Gets a list of bid recommendations for keyword, product, or auto targeting expressions.

        body: | REQUIRED {'description': 'An array of ad groups.}'

            | '**adGroupId**': *number*, {'description': 'The ad group identifier.'}
            | expressions {
            | '**value**': *string*, {'description': 'The expression value.'}
            | '**type**': *string*, {'description': 'The type of targeting expression', 'Enum': '[ queryBroadMatches, queryPhraseMatches, queryExactMatches, asinCategorySameAs, asinBrandSameAs, asinPriceLessThan, asinPriceBetween, asinPriceGreaterThan, asinReviewRatingLessThan, asinReviewRatingBetween, asinReviewRatingGreaterThan, asinSameAs, queryBroadRelMatches, queryHighRelMatches, asinSubstituteRelated, asinAccessoryRelated, asinAgeRangeSameAs, asinGenreSameAs, asinIsPrimeShippingEligible ]'}
            | }


        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)
