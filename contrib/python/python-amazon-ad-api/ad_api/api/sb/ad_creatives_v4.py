from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class AdCreativesV4(Client):
    @sp_endpoint('/sb/ads/creatives/brandVideo', method='POST')
    def create_video_ad_creative(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Creates a new version of an existing creative for given Sponsored Brands Ad by supplying brand video creative content.

        Request Body [Required]
            | **adId** (string) : The unique ID of a Sponsored Brands ad.
            | **creative** ({}) :
                | **asins** ([string]) : An array of ASINs associated with the creative.
                | **brandLogoCrop** ({}): [optional] Asset cropping attributes
                    | **top** (int) : The highest pixel from which to begin cropping.
                    | left (int) : The leftmost pixel from which to begin cropping.
                    | width (int) : The number of pixels to crop rightwards from the value specified as left.
                    | height (int) : The number of pixels to crop down from the value specified as top.
                | **brandName** (string) : The displayed brand name in the ad headline. Maximum length is 30 characters. See the policy for headline requirements.
                | **landingPage** ({}) [optional] :
                    | asins ([string]) : The list of asins on the landingPage If type is PRODUCT_LIST. A minimum of 3 asins are required. For the 'PRODUCT_LIST' type, the asins property is mandatory, and the url should not be included.
                    | type (string) : Supported types are PRODUCT_LIST, STORE, DETAIL_PAGE, CUSTOM_URL. More could be added in future.
                    | url (string) : The url of the landingPage. When including the 'asins' property in the request, do not include this property, as they are mutually exclusive. For the PRODUCT_LIST type, the asins property is mandatory, and the url should not be included.
                | **consentToTranslate** (boolean) [optional] : If set to true and the headline and/or video are not in the marketplace's default language, Amazon will attempt to translate them to the marketplace's default language.
                | **videoAssetIds** ([string]) : The assetIds of the original videos submitted by the advertiser.
                | **brandLogoAssetId** (string) : The identifier of the brand logo image from the brand store's asset library.
                | **headline** (string) : The headline text.

        Returns
            ApiResponse

        """

        json_version = 'application/vnd.sbAdCreativeResource.v' + str(version) + "+json"
        headers = {'Accept': json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/ads/creatives/video', method='POST')
    def create_new_version_video_ad_creative(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Creates a new version of an existing creative for given Sponsored Brands ad by supplying video creative content.

        Request Body [Required]
            | **adId** (string) : The unique ID of a Sponsored Brands ad.
            | **creative** ({}) :
                | **consentToTranslate** (boolean) [optional] : If set to true and the headline and/or video are not in the marketplace's default language, Amazon will attempt to translate them to the marketplace's default language.
                | **videoAssetIds** ([string]) : The assetIds of the original videos submitted by the advertiser.

        Returns
            ApiResponse

        """

        json_version = 'application/vnd.sbAdCreativeResource.v' + str(version) + "+json"
        headers = {'Accept': json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/ads/creatives/productCollection', method='POST')
    def create_new_version_product_collection_ad_creative(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Creates a new version of creative for given Sponsored Brands ad by supplying product collection creative content.

        Request Body [Required]
            | **adId** (string) : The unique ID of a Sponsored Brands ad.
            | **creative** ({}) :
                | **asins** ([string]) : An array of ASINs associated with the creative.
                | **brandLogoCrop** ({}): [optional] Asset cropping attributes
                    | top (int) : The highest pixel from which to begin cropping.
                    | left (int) : The leftmost pixel from which to begin cropping.
                    | width (int) : The number of pixels to crop rightwards from the value specified as left.
                    | height (int) : The number of pixels to crop down from the value specified as top.
                | **brandName** (string) : The displayed brand name in the ad headline. Maximum length is 30 characters. See the policy for headline requirements.
                | **customImageAssetId** (string) [optional] : The identifier of the Custom image from the Store assets library. See the policy for more information on what constitutes a valid Custom image.
                | **customImageCrop** ({}) [optional] : Asset cropping attributes
                    | top (int) : The highest pixel from which to begin cropping.
                    | left (int) : The leftmost pixel from which to begin cropping.
                    | width (int) : The number of pixels to crop rightwards from the value specified as left.
                    | height (int) : The number of pixels to crop down from the value specified as top.
                | **brandLogoAssetId** (string) : The identifier of the brand logo image from the brand store's asset library.
                | **headline** (string) : The headline text.

        Returns
            ApiResponse

        """

        json_version = 'application/vnd.sbAdCreativeResource.v' + str(version) + "+json"
        headers = {'Accept': json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/ads/creatives/productCollection', method='POST')
    def create_new_version_store_spotlight_ad_creative(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Creates a new version of creative for given Sponsored Brands ad by supplying product collection creative content.

        Request Body [Required]
            | **adId** (string) : The unique ID of a Sponsored Brands ad.
            | **creative** ({})
                | **brandLogoCrop** ({}): [optional] Asset cropping attributes
                    | **top** (int) : The highest pixel from which to begin cropping.
                    | left (int) : The leftmost pixel from which to begin cropping.
                    | width (int) : The number of pixels to crop rightwards from the value specified as left.
                    | height (int) : The number of pixels to crop down from the value specified as top.
                | **brandName** (string) : The displayed brand name in the ad headline. Maximum length is 30 characters. See the policy for headline requirements.
                | **subpages** ([{}]) An array of subpages
                    | pageTitle (string)
                    | asin (string)
                    | url (string)
                | **landingPage** ({}) [optional]
                    | asins ([string]) : The list of asins on the landingPage If type is PRODUCT_LIST. A minimum of 3 asins are required. For the 'PRODUCT_LIST' type, the asins property is mandatory, and the url should not be included.
                    | type (string) : Supported types are PRODUCT_LIST, STORE, DETAIL_PAGE, CUSTOM_URL. More could be added in future.
                    | url (string) : The url of the landingPage. When including the 'asins' property in the request, do not include this property, as they are mutually exclusive. For the PRODUCT_LIST type, the asins property is mandatory, and the url should not be included.
                | **consentToTranslate** (boolean) [optional] : If set to true and the headline and/or video are not in the marketplace's default language, Amazon will attempt to translate them to the marketplace's default language.
                | **brandLogoAssetId** (string) : The identifier of the brand logo image from the brand store's asset library.
                | **headline** (string) : The headline text.

        Returns
            ApiResponse

        """

        json_version = 'application/vnd.sbAdCreativeResource.v' + str(version) + "+json"
        headers = {'Accept': json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/ads/creatives/list', method='POST')
    def list_ad_creatives(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Creates a new version of creative for given Sponsored Brands ad by supplying product collection creative content.

        Request Body [Required]
            | **creativeTypeFilter** ([string]): [optional] Filters creatives by optional creative type. By default, you can list all creative versions regardless of creative type. Items Enum: "PRODUCT_COLLECTION" "STORE_SPOTLIGHT" "VIDEO" "BRAND_VIDEO"
            | **adId** (string): The unique ID of a Sponsored Brands ad.
            | **nextToken** (string): [optional] Token value allowing to navigate to the next response page.
            | **maxResults** (int): [optional] Number of records to include in the paginated response. Defaults to max page size for given API.
            | **creativeVersionFilter** ([string]): [optional] Filters creatives by optional creative version.
            | **creativeStatusFilter** ([string]): [optional] Filters creatives by optional creative status. tems Enum: "SUBMITTED_FOR_MODERATION" "PENDING_TRANSLATION" "PENDING_MODERATION_REVIEW" "APPROVED_BY_MODERATION" "REJECTED_BY_MODERATION" "PUBLISHED"

        Returns
            ApiResponse

        """

        json_version = 'application/vnd.sbAdCreativeResource.v' + str(version) + "+json"
        headers = {'Accept': json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
