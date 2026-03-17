from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class Targets(Client):
    """
    Use the Amazon Advertising API for Sponsored Brands for campaign, ad group, keyword, negative keyword, drafts, Stores, landing pages, and Brands management operations. For more information about Sponsored Brands, see the Sponsored Brands Support Center. For onboarding information, see the account setup topic.
    """

    @sp_endpoint('/sb/targets/list', method='POST')
    def list_products_targets(self, version: float = 3.2, **kwargs) -> ApiResponse:
        r"""
        Gets a list of product targets associated with the client identifier passed in the authorization header, filtered by specified criteria.

        Request Body
            | '**nextToken**': *string*, {'description': 'Operations that return paginated results include a pagination token in this field. To retrieve the next page of results, call the same operation and specify this token in the request. If the NextToken field is empty, there are no further results.'}
            | '**maxResults**': *number*, {'description': 'The identifier of the ad group to which this keyword is associated.'}
            | '**filters**': *string*, {'Restricts results to targets with the specified filters. Filters are inclusive. Filters are joined using 'and' logic. Specify one type of each filter. Specifying multiples of the same type of filter results in an error.'}

                | '**filterType**': *string*, {'CREATIVE_TYPE': '[ CREATIVE_TYPE ]'}

                | '**filterType**': *string*, {'TARGETING_STATE': '[ archived, paused, pending, enabled ]'}

                | '**filterType**': *string*, {'CAMPAIGN_ID': '[ CAMPAIGN_ID ]'}

                | '**filterType**': *string*, {'AD_GROUP_ID': '[ SBAdGroupId ]'}


        Returns:
            ApiResponse

        """
        json_version = 'application/vnd.sblisttargetsresponse.v' + str(version) + "+json"
        headers = {'Accept': json_version}
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs, headers=headers)

    @sp_endpoint('/sb/targets', method='PUT')
    def edit_products_targets(self, **kwargs) -> ApiResponse:
        r"""
        Updates one or more targets.

        Request Body
            | '**targetId**': *integer($int64)*, {'description': 'The identifier of the target.'}
            | '**adGroupId**': *integer($int64)*, {'description': 'The identifier of the ad group to which the target is associated.'}
            | '**campaignId**': *integer($int64)*, {'description': 'The identifier of the campaign to which the target is associated.'}
            | '**state**': *string*, {'values': '[ enabled, paused, pending, archived, draft ]'}
            | '**bid**': *number*, {'description': 'The associated bid. Note that this value must be less than the budget associated with the Advertiser account. For more information, see supported features.'}

        Returns:
            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sb/targets', method='POST')
    def create_products_targets(self, **kwargs) -> ApiResponse:
        r"""
        Create one or more targets.

        Request Body
            | '**adGroupId**': *integer($int64)*, {'description': 'The identifier of the ad group to which the target is associated.'}
            | '**campaignId**': *integer($int64)*, {'description': 'The identifier of the campaign to which the target is associated.'}
            | '**expressions**': *SBExpression*, {'type': 'asinCategorySameAs, asinBrandSameAs, asinPriceLessThan, asinPriceBetween, asinPriceGreaterThan, asinReviewRatingLessThan, asinReviewRatingBetween, asinReviewRatingGreaterThan, asinSameAs', 'values': 'The text of the targeting expression. The - token defines a range. For example, 2-4 defines a range of 2, 3, and 4.'}
            | '**bid**': *number*, {'description': 'The associated bid. Note that this value must be less than the budget associated with the Advertiser account. For more information, see supported features.'}

        Returns:
            ApiResponse
        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sb/targets/{}', method='GET')
    def get_products_target(self, targetId, **kwargs) -> ApiResponse:
        r"""
        Gets a target specified by identifier.

        Keyword Args
            path **targetId**:*number* | Required. The identifier of an existing target.

        Returns
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), targetId), params=kwargs)

    @sp_endpoint('/sb/targets/{}', method='DELETE')
    def delete_products_target(self, targetId, **kwargs) -> ApiResponse:
        r"""
        Archives a target specified by identifier. Note that archiving is permanent, and once a target has been archived it can't be made active again.

        Keyword Args
            path **targetId**:*number* | Required. The identifier of an existing target.

        Returns
            ApiResponse


        """
        return self._request(fill_query_params(kwargs.pop('path'), targetId), params=kwargs)
