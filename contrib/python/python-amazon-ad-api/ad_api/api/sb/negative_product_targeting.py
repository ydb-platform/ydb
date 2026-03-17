from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class NegativeTargets(Client):
    """
    Use the Amazon Advertising API for Sponsored Brands for campaign, ad group, keyword, negative keyword, drafts, Stores, landing pages, and Brands management operations. For more information about Sponsored Brands, see the Sponsored Brands Support Center. For onboarding information, see the account setup topic.
    """

    @sp_endpoint('/sb/negativeTargets/list', method='POST')
    def list_negative_targets(self, **kwargs) -> ApiResponse:
        r"""
        Gets a list of product negative targets associated with the client identifier passed in the authorization header, filtered by specified criteria.

        Request Body
            | '**nextToken**': *string*, {'description': 'Operations that return paginated results include a pagination token in this field. To retrieve the next page of results, call the same operation and specify this token in the request. If the NextToken field is empty, there are no further results.'}
            | '**maxResults**': *number*, {'description': 'The identifier of the ad group to which this keyword is associated.'}
            | '**filters**': *string*, {'Restricts results to targets with the specified filters. Filters are inclusive. Filters are joined using 'and' logic. Specify one type of each filter. Specifying multiples of the same type of filter results in an error.'}

                | '**filterType**': *string*, {'CREATIVE_TYPE': '[ productCollection, video ]'}

                | '**filterType**': *string*, {'TARGETING_STATE': '[ enabled, pending, archived ]'}

                | '**filterType**': *string*, {'CAMPAIGN_ID': '[ CAMPAIGN_ID ]'}

                | '**filterType**': *string*, {'AD_GROUP_ID': '[ SBAdGroupId ]'}


        Returns:
            ApiResponse
        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sb/negativeTargets', method='PUT')
    def edit_negative_targets(self, **kwargs) -> ApiResponse:
        r"""
        Updates one or more negative targeting clauses.

        Request Body
            | '**targetId**': *integer($int64)*, {'description': 'The target identifier.'}
            | '**adGroupId**': *integer($int64)*, {'description': 'The identifier of an existing ad group. The newly created target is associated to this ad group.'}
            | '**state**': *string*, {'values': '[ enabled, paused, pending, archived, draft ]'}

        Returns:
            ApiResponse
        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sb/negativeTargets', method='POST')
    def create_negative_targets(self, **kwargs) -> ApiResponse:
        r"""
        Creates one or more targeting expressions.

        Request Body

            | '**adGroupId**': *integer($int64)*, {'description': 'The identifier of the ad group to which the target is associated.'}
            | '**campaignId**': *integer($int64)*, {'description': 'The identifier of the campaign to which the target is associated.'}
            | '**expressions**': *string*, {'Restricts results to targets with the specified filters. Filters are inclusive. Filters are joined using 'and' logic. Specify one type of each filter. Specifying multiples of the same type of filter results in an error.'}

                | '**type**': *string*, {'values': 'asinBrandSameAs, asinSameAs'}

                | '**value**': *string*, {'description': 'The text of the negative expression.'}

        Returns:
            ApiResponse
        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sb/negativeTargets/{}', method='GET')
    def get_negative_target(self, negativeTargetId, **kwargs) -> ApiResponse:
        r"""
        Get a negative targeting clause specified by identifier.

        Keyword Args
            path **negativeTargetId**:*number* | Required. The identifier of an existing negative target.

        Returns
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), negativeTargetId), params=kwargs)

    @sp_endpoint('/sb/negativeTargets/{}', method='DELETE')
    def delete_negative_target(self, negativeTargetId, **kwargs) -> ApiResponse:
        r"""
        Archives a negative targeting clause.

        Keyword Args
            path **negativeTargetId**:*number* | Required. The identifier of an existing negative target.

        Returns
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), negativeTargetId), params=kwargs)
