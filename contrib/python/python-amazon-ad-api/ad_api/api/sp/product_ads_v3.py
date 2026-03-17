from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class ProductAdsV3(Client):
    """
    Version 3 of the Sponsored Products API
    """

    @sp_endpoint('/sp/productAds/list', method='POST')
    def list_product_ads(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        Lists Sponsored Products Ads.

        Request Body (optional): Include the body for specific filtering, or leave empty to get all ad groups.
            | **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0
            | **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.
            | **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.
            | **adGroupIdFilter**:*string* | Optional. Restrict results to keywords associated with ad groups specified by identifier in the comma-delimited list.
            | **adIdFilter**:*string* | Optional. Restrict results to product ads associated with the product ad identifiers specified in the comma-delimited list.
             | **next_token** (string) Token value allowing to navigate to the next response page. [optional]
            | **max_results** (int) Number of records to include in the paginated response. Defaults to max page size for given API. Minimum 10 and a Maximum of 100 [optional]
            | **includeExtendedDataFields** (boolean) Whether to get entity with extended data fields such as creationDate, lastUpdateDate, servingStatus [optional]

        Returns:
            ApiResponse
        """

        json_version = 'application/vnd.spproductAd.v' + str(version) + "+json"

        headers = {"Accept": json_version, "Content-Type": json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/productAds', method='POST')
    def create_product_ads(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Create one or more SP Product Ads.

        Request body: (Required) A list of product ads for creation. Note that the SKU field is used by sellers and the ASIN field is used by vendors.
            | '**campaignId**': (*string*) The campaign identifier.
            | **customText** (*string*) : The custom text to use for creating a custom text ad for the associated ASIN. Defined only for KDP Authors and Book Vendors in US marketplace.
            | '**adGroupId**': (*string*) The ad group identifier.
            | '**sku**': (*string*) The SKU associated with the product. Defined for seller accounts only.
            | '**asin**': (*string*) The ASIN associated with the product. Defined for vendors only.
            | '**state**': (*string*) The current resource state. 'Enum':[ enabled, paused, archived ]

        Prefer (header) : You can use the prefer header to specify whether you want the full object representation as
        part of the response. If you don’t specify the prefer header, you will see just the ID of the created entity in the response.

        Returns:
            ApiResponse
        """

        json_version = "application/vnd.spProductAd.v" + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/productAds', method='PUT')
    def edit_product_ads(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Updates one or more product ads specified by identifier.

        Request Body: (REQUIRED) A list of product ad objects with updated values for the state field.
            | **adId**: *string* The identifier of an existing product ad to update.
            | **state**: *string* The current resource state.', 'Enum': [ enabled, paused, archived ]

        Prefer (header) : You can use the prefer header to specify whether you want the full object representation as
        part of the response. If you don’t specify the prefer header, you will see just the ID of the created entity in the response.

        Returns:
            ApiResponse

        """

        json_version = "application/vnd.spProductAd.v" + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/productAds/delete', method='POST')
    def delete_product_ads(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        Delete one or multiple sponsored product ads.

        Request Body (required) : a dictionary to filter with ad ids that should be deleted. Sets the state of a
        specified product ad to archived
            | **adIdFilter** ({})
                | include [string] : string list of ad object identifier.

        Returns
            | ApiResponse
        """

        json_version = "application/vnd.spProductAd.v" + str(version) + "+json"

        headers = {"Accept": json_version, "Content-Type": json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
