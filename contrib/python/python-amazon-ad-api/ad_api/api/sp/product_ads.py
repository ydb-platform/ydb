from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class ProductAds(Client):
    @sp_endpoint('/v2/sp/productAds', method='GET')
    @Utils.deprecated
    def list_product_ads(self, **kwargs) -> ApiResponse:
        r"""
        list_product_ads(self, \*\*kwargs) -> ApiResponse

        Gets an array of campaigns.

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.

            query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.

            query **adGroupIdFilter**:*string* | Optional. Restricts results to keywords associated with ad groups specified by identifier in the comma-delimited list.

            query **adIdFilter**:*string* | Optional. Restricts results to product ads associated with the product ad identifiers specified in the comma-delimited list.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/v2/sp/productAds/extended', method='GET')
    @Utils.deprecated
    def list_product_ads_extended(self, **kwargs) -> ApiResponse:
        r"""
        list_product_ads_extended_request(self, \*\*kwargs) -> ApiResponse

        Gets extended data for a list of product ads filtered by specified criteria.

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.

            query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.

            query **adGroupIdFilter**:*string* | Optional. Restricts results to keywords associated with ad groups specified by identifier in the comma-delimited list.

            query **adIdFilter**:*string* | Optional. Restricts results to product ads associated with the product ad identifiers specified in the comma-delimited list.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/v2/sp/productAds/{}', method='GET')
    @Utils.deprecated
    def get_product_ad(self, adId, **kwargs) -> ApiResponse:
        r"""

        get_product_ad_request(self, adId, \*\*kwargs) -> ApiResponse

        Gets a product ad specified by identifier.

            path **adId**:*number* | Required. A product ad identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), adId), params=kwargs)

    @sp_endpoint('/v2/sp/productAds/extended/{}', method='GET')
    @Utils.deprecated
    def get_product_ad_extended(self, adId, **kwargs) -> ApiResponse:
        r"""

        get_product_ad_extended(self, adId, **kwargs) -> ApiResponse

        Gets extended data for a product ad specified by identifier.

            path **adId**:*number* | Required. A product ad identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), adId), params=kwargs)

    @sp_endpoint('/v2/sp/productAds/{}', method='DELETE')
    @Utils.deprecated
    def delete_product_ad(self, adId, **kwargs) -> ApiResponse:
        r"""

        delete_product_ad(self, adId, \*\*kwargs) -> ApiResponse

        Sets the state of a specified product ad to archived. Note that once the state is set to archived it cannot be changed.

            path **adId**:*number* | Required. A product ad identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), adId), params=kwargs)

    @sp_endpoint('/v2/sp/productAds', method='PUT')
    @Utils.deprecated
    def edit_product_ads(self, **kwargs) -> ApiResponse:
        r"""
        edit_product_ads(self, \*\*kwargs) -> ApiResponse

        Updates one or more product ads specified by identifier.

        body: | REQUIRED {'description': 'A list of product ad objects with updated values for the state field.}'

            | '**adId**': *number*, {'description': 'The identifier of an existing campaign to update.'}
            | '**state**': *string*, {'description': 'The current resource state.', 'Enum': '[ enabled, paused, archived ]'}

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/v2/sp/productAds', method='POST')
    @Utils.deprecated
    def create_product_ads(self, **kwargs) -> ApiResponse:
        r"""
        create_product_ads(self, \*\*kwargs) -> ApiResponse

        Creates one or more product ads.

        body: | REQUIRED {'description': 'A list of product ads for creation. Note that the SKU field is used by sellers and the ASIN field is used by vendors.'}

            | '**campaignId**': *number*, {'description': 'The campaign identifier.'}
            | '**adGroupId**': *number*, {'description': 'The ad group identifier.'}
            | '**sku**': *string*, {'description': 'The SKU associated with the product. Defined for seller accounts only.'}
            | '**asin**': *string*, {'description': 'The ASIN associated with the product. Defined for vendors only.'}
            | '**state**': *string*, {'description': 'The current resource state.', 'Enum': '[ enabled, paused, archived ]'}

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)
