from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class NegativeTargets(Client):
    @sp_endpoint('/v2/sp/negativeTargets', method='POST')
    @Utils.deprecated
    def create_negative_targets(self, **kwargs) -> ApiResponse:
        r"""
        create_products_targets(self, \*\*kwargs) -> ApiResponse:

        Creates one or more targeting expressions.

        body: | REQUIRED {'description': 'An array of asins objects.}'

            | '**campaignId**': *number*, {'description': 'The identifier of the campaign to which this negative target is associated.'}
            | '**adGroupId**': *number*, {'description': 'The identifier of the ad group to which this negative target is associated.'}
            | '**state**': *number*, {'description': 'The current resource state. [ enabled, paused, archived ]'}
            | '**expression**'
            |       '**value**': *string*, {'description': 'The expression value. '}
            |       '**type**': *string*, {'description': '[ queryBroadMatches, queryPhraseMatches, queryExactMatches, asinCategorySameAs, asinBrandSameAs, asinPriceLessThan, asinPriceBetween, asinPriceGreaterThan, asinReviewRatingLessThan, asinReviewRatingBetween, asinReviewRatingGreaterThan, asinSameAs, queryBroadRelMatches, queryHighRelMatches, asinSubstituteRelated, asinAccessoryRelated, asinAgeRangeSameAs, asinGenreSameAs, asinIsPrimeShippingEligible ]'}
            | '**expressionType**': *string*, {'description': '[ auto, manual ]'}

        Returns:

            ApiResponse


        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/v2/sp/negativeTargets', method='PUT')
    @Utils.deprecated
    def edit_negative_targets(self, **kwargs) -> ApiResponse:
        r"""
        edit_negative_targets(self, \*\*kwargs) -> ApiResponse:

        Updates one or more negative targeting clauses.

        body: | REQUIRED {'description': 'An array of asins objects.}'

            | '**campaignId**': *number*, {'description': 'The identifier of the campaign to which this negative target is associated.'}
            | '**adGroupId**': *number*, {'description': 'The identifier of the ad group to which this negative target is associated.'}
            | '**state**': *number*, {'description': 'The current resource state. [ enabled, paused, archived ]'}
            | '**expression**'
            |       '**value**': *string*, {'description': 'The expression value. '}
            |       '**type**': *string*, {'description': '[ queryBroadMatches, queryPhraseMatches, queryExactMatches, asinCategorySameAs, asinBrandSameAs, asinPriceLessThan, asinPriceBetween, asinPriceGreaterThan, asinReviewRatingLessThan, asinReviewRatingBetween, asinReviewRatingGreaterThan, asinSameAs, queryBroadRelMatches, queryHighRelMatches, asinSubstituteRelated, asinAccessoryRelated, asinAgeRangeSameAs, asinGenreSameAs, asinIsPrimeShippingEligible ]'}
            | '**expressionType**': *string*, {'description': '[ auto, manual ]'}

        Returns:

            ApiResponse


        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/v2/sp/negativeTargets', method='GET')
    @Utils.deprecated
    def list_negative_targets(self, **kwargs) -> ApiResponse:
        r"""
        list_negative_targets(self, \*\*kwargs) -> ApiResponse

        Gets a list of negative targeting clauses filtered by specified criteria.

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.

            query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.

            query **adGroupIdFilter**:*string* | Optional. Restricts results to keywords associated with ad groups specified by identifier in the comma-delimited list.

            query **targetIdFilter**:*string* | Optional. A comma-delimited list of target identifiers.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/v2/sp/negativeTargets/{}', method='GET')
    @Utils.deprecated
    def get_negative_target(self, targetId, **kwargs) -> ApiResponse:
        r"""

        get_negative_targets(self, targetId, \*\*kwargs) -> ApiResponse

        Get a negative targeting clause specified by identifier.

            path **targetId**:*number* | Required. The target identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), targetId), params=kwargs)

    @sp_endpoint('/v2/sp/negativeTargets/{}', method='DELETE')
    @Utils.deprecated
    def delete_negative_targets(self, targetId, **kwargs) -> ApiResponse:
        r"""

        delete_negative_targets(self, targetId, \*\*kwargs) -> ApiResponse

        Archives a negative targeting clause.

            path **targetId**:*number* | Required. The target identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), targetId), params=kwargs)

    @sp_endpoint('/v2/sp/negativeTargets/extended', method='GET')
    @Utils.deprecated
    def list_negative_targets_extended(self, **kwargs) -> ApiResponse:
        r"""
        list_negative_targets_extended(self, \*\*kwargs) -> ApiResponse

        Gets a list of negative targeting clauses filtered by specified criteria.

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.

            query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.

            query **adGroupIdFilter**:*string* | Optional. Restricts results to keywords associated with ad groups specified by identifier in the comma-delimited list.

            query **targetIdFilter**:*string* | Optional. A comma-delimited list of target identifiers.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/v2/sp/negativeTargets/extended/{}', method='GET')
    @Utils.deprecated
    def get_negative_target_extended(self, targetId, **kwargs) -> ApiResponse:
        r"""

        get_negative_target_extended(self, targetId, \*\*kwargs) -> ApiResponse

        Get a negative targeting clause specified by identifier.

            path **targetId**:*number* | Required. The target identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), targetId), params=kwargs)
