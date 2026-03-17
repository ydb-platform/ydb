from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class ProductAds(Client):
    """Sponsored Display Product Ads

    Documentation: https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Product%20ads/

    The Sponsored Display API supports creation of reports for campaigns, ad groups, product ads, targets, and asins. Create a ReportRequest object specifying the fields corresponding to performance data metrics to include in the report. See ReportRequest object for more information about the performance data metric fields and their descriptions.
    """

    @sp_endpoint('/sd/productAds', method='GET')
    def list_product_ads(self, **kwargs) -> ApiResponse:
        r"""Gets a list of product ads.

        Gets an array of ProductAd objects for a requested set of Sponsored Display product ads. Note that the ProductAd object is designed for performance, and includes a small set of commonly used fields to reduce size. If the extended set of fields is required, use a product ad operation that returns the ProductAdResponseEx object.

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.

            query **adIdFilter**:*string* | Optional. Restricts results to product ads associated with the product ad identifiers specified in the comma-delimited list.

            query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.

            query **adGroupIdFilter**:*string* | Optional. Restricts results to keywords associated with ad groups specified by identifier in the comma-delimited list.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sd/productAds', method='PUT')
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

    @sp_endpoint('/sd/productAds', method='POST')
    def create_product_ads(self, **kwargs) -> ApiResponse:
        r"""
        create_product_ads(self, \*\*kwargs) -> ApiResponse

        Creates one or more product ads.

        body: | REQUIRED {'description': 'An array of ProductAd objects. For each object, specify required fields and their values. Required fields are adGroupId, SKU (for sellers) or ASIN (for vendors), and state'. Maximum length of the array is 100 objects.'}

            | '**state**': *string*, {'description': 'The current resource state.', 'Enum': '[ enabled, paused, archived ]'}
            | '**adGroupId**': *integer($int64)*, {'description': 'The identifier of the ad group.'}
            | '**campaignId**': *integer($int64)*, {'description': 'The identifier of the campaign.'}
            | '**asin**': *string*, {'description': 'The ASIN associated with the product. Defined for vendors only.'}
            | '**sku**': *string*, {'description': 'The SKU associated with the product. Defined for seller accounts only.'}

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sd/productAds/{}', method='GET')
    def get_product_ad(self, adId, **kwargs) -> ApiResponse:
        r"""

        get_product_ad_request(self, adId, \*\*kwargs) -> ApiResponse

        Gets a product ad specified by identifier.

            path **adId**:*number* | Required. A product ad identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), adId), params=kwargs)

    @sp_endpoint('/sd/productAds/{}', method='DELETE')
    def delete_product_ad(self, adId, **kwargs) -> ApiResponse:
        r"""

        delete_product_ad(self, adId, \*\*kwargs) -> ApiResponse

        Sets the state of a specified product ad to archived. Note that once the state is set to archived it cannot be changed.

            path **adId**:*number* | Required. A product ad identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), adId), params=kwargs)

    @sp_endpoint('/sd/productAds/extended', method='GET')
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

    @sp_endpoint('/sd/productAds/extended/{}', method='GET')
    def get_product_ad_extended(self, adId, **kwargs) -> ApiResponse:
        r"""

        get_product_ad_extended(self, adId, **kwargs) -> ApiResponse

        Gets extended data for a product ad specified by identifier.

            path **adId**:*number* | Required. A product ad identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), adId), params=kwargs)
