from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class NegativeTargetsV3(Client):
    @sp_endpoint('/sp/negativeTargets/list', method='POST')
    def list_negative_product_targets(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        Listing negative product targets.

        Request Body (optional)

        Returns
            ApiResponse
        """
        json_version = 'application/vnd.spNegativeTargetingClause.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/negativeTargets', method='POST')
    def create_negative_product_targets(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Creating negative product targets.

        Request Body (required)
            | '**campaignId**': *number*, {'description': 'The identifier of the campaign to which this negative target is associated.'}
            | '**adGroupId**': *number*, {'description': 'The identifier of the ad group to which this negative target is associated.'}
            | '**state**': *number*, {'description': 'The current resource state. [ enabled, paused, archived ]'}
            | '**expression**'
                |  '**value**': *string*, {'description': 'The expression value. '}
                |  '**type**': *string*, {'description': The type of negative targeting expression. You can only specify values for the following predicates: 'Enum : [ ASIN_BRAND_SAME_AS, ASIN_SAME_AS ]'}

        Returns
            ApiResponse
        """
        json_version = 'application/vnd.spNegativeTargetingClause.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/negativeTargets', method='PUT')
    def edit_negative_product_targets(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Updating negative product targets.

        Request Body (required)
            | **targetId** : *string*, The target identifier
            | '**state**': *string*, The current resource state. [ enabled, paused, archived ]
            | '**expression**'
            |       '**value**': *string*, The expression value.
            |       '**type**': *string*, The type of nagative targeting expression. You can only specify values for the following predicates: Enum : [ASIN_BRAND_SAME_AS, ASIN_SAME_AS]

        Returns
            ApiResponse
        """
        json_version = 'application/vnd.spNegativeTargetingClause.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/negativeTargets/delete', method='POST')
    def delete_negative_product_targets(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        Deleting negative product targets.

        Request Body (required)
            | **keywordIdFilter** {} : Filter negative targets by the list of objectIds
                include [string] : list of negativeTargetIds as String to be used as filter. MinItems : 0, MaxItems :1000


        Returns
            ApiResponse
        """
        json_version = 'application/vnd.spNegativeTargetingClause.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    # MOVE TO NegativeTargetsV3

    # Maybe need to move to NegativeTargetsV3 and add the prefer header boolen and param the version in case upper version amazon release
    @sp_endpoint('/sp/negativeTargets/brands/search', method='POST')
    def list_negative_targets_brands_search(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Returns brands related to keyword input for negative targeting.

            | header **Prefer**:*string* | Used to indicate the behavior preferred by the client but is not required for successful completion of the request. Supported values will be updated in the future.

        Returns:

            ApiResponse


        """
        json_version = 'application/vnd.spproducttargeting.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})
        # contentType = 'application/vnd.spproducttargeting.v3+json'
        # headers = {'Content-Type': contentType}
        return self._request(
            kwargs.pop('path'),
            data=Utils.convert_body(kwargs.pop('body'), False),
            params=kwargs,
            headers=headers or False,
        )

    # Maybe need to move to NegativeTargetsV3 and add the prefer header boolen and param the version in case upper version amazon release
    @sp_endpoint('/sp/negativeTargets/brands/recommendations', method='GET')
    def list_negative_targets_brands_recommendations(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Returns brands recommended for negative targeting. Only available for Sellers and Vendors. These recommendations include your own brands because targeting your own brands usually results in lower performance than targeting competitors' brands.

            | header **Prefer**:*string* | Used to indicate the behavior preferred by the client but is not required for successful completion of the request. Supported values will be updated in the future.

        Returns:

            ApiResponse


        """
        json_version = 'application/vnd.spproducttargeting.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})
        return self._request(kwargs.pop('path'), params=kwargs, headers=headers or False)
