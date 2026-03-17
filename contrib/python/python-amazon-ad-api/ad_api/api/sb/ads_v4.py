from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class AdsV4(Client):
    @sp_endpoint('/sb/v4/ads/list', method='POST')
    def list_ads(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Lists all Sponsored Brands ads.

        Request Body (optional)
        | **campaignIdFilter** (dict) : Filter entities by the list of objectIds.
        | **stateFilter** (dict) : Filter entities by state.
        | **maxResults** (int) : Number of records to include in the paginated response. Defaults to max page size for given API.
        | **nextToken** (string) : Token value allowing to navigate to the next response page.
        | **adIdFilter** (dict) : Filter entities by the list of objectIds.
        | **adGroupIdFilter** (dict) : Filter entities by the list of objectIds.
        | **nameFilter** (dict) : Filter entities by name.


        Returns
            ApiResponse

        """

        json_version = 'application/vnd.sbadresource.v' + str(version) + "+json"
        headers = {'Accept': json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/v4/ads/video', method='POST')
    def create_video_ads(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Creates Sponsored Brand video ads.

        Request Body (required)
        | **ads** ([{}]) :
            **name** (string) : The name of the ad
            **state** (CreateOrUpdateEntityState > String) Entity state for create or update operation. Enum : [ENABLED, PAUSED]
            **adGroupId** (string) : The adGroup identifier.
            **creative** (dict) :
                asins ([string])  [optional]
                videoAssetIds ([string])  [optional]

        Returns
            ApiResponse

        """

        json_version = 'application/vnd.sbadresource.v' + str(version) + "+json"
        headers = {'Accept': json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/v4/ads/productCollection', method='POST')
    def create_product_collection_ads(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Creates Sponsored Brands product collection ads.

        Request Body (required)
        | **ads** ([{}]) :
            **landingPage** (dict) :
                asins [string] [optional]
                pageType (LadingPageType > String ) : The type of landing page, such as store page, product list (simple landing page), custom url. [optional]
                url (string) : URL of an existing simple landing page or Store page. [optional]
            **name** (string) : The name of the ad
            **state** (CreateOrUpdateEntityState > String) Entity state for create or update operation. Enum : [ENABLED, PAUSED]
            **adGroupId** (string) : The adGroup identifier.
            **creative** (dict) :
                brandLogoCrop (string) [optional]
                brandName (string) [optional]
                customImageAssetId	(string)
                customImageCrop ({int})  [optional]
                brandLogoAssetID (string) [optional]
                headline (string) The headline text [optional]

        Returns
            ApiResponse

        """

        json_version = 'application/vnd.sbadresource.v' + str(version) + "+json"
        headers = {'Accept': json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/v4/ads/brandVideo', method='POST')
    def create_brand_video_ads(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Creates Sponsored Brands brand video ads.

        Request Body (required)
        | **ads** ([{}]) :
            **landingPage** (dict) :
                asins [string] [optional]
                pageType (LadingPageType > String ) : The type of landing page, such as store page, product list (simple landing page), custom url. [optional]
                url (string) : URL of an existing simple landing page or Store page. [optional]
            **name** (string) : The name of the ad
            **state** (CreateOrUpdateEntityState > String) Entity state for create or update operation. Enum : [ENABLED, PAUSED]
            **adGroupId** (string) : The adGroup identifier.
            **creative** (dict) :
                asins ([string])
                brandName (string) [optional]
                brandLogoCrop ({}) [optional]
                videoAssetIds ([string]) [optional]
                brandLogoAssetID (string) [optional]
                headline (string) The headline text [optional]

        Returns
            ApiResponse
        """

        json_version = 'application/vnd.sbadresource.v' + str(version) + "+json"
        headers = {'Accept': json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/v4/ads/storeSpotlight', method='POST')
    def create_store_spotlight_ads(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Creates Sponsored Brands store spotlight ads.

        Request Body (required)
        | **ads** ([{}]) :
            **landingPage** (dict) :
                asins [string] [optional]
                pageType (LadingPageType > String ) : The type of landing page, such as store page, product list (simple landing page), custom url. [optional]
                url (string) : URL of an existing simple landing page or Store page. [optional]
            **name** (string) : The name of the ad
            **state** (CreateOrUpdateEntityState > String) Entity state for create or update operation. Enum : [ENABLED, PAUSED]
            **adGroupId** (string) : The adGroup identifier.
            **creative** (dict) :
                brandLogoCrop (string) [optional]
                brandName (string) [optional]
                subpages (string) [optional]
                brandLogoAssetID (string) [optional]
                headline (string) The headline text [optional]

        Returns
            ApiResponse
        """

        json_version = 'application/vnd.sbadresource.v' + str(version) + "+json"
        headers = {'Accept': json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/v4/ads', method='PUT')
    def update_ads(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Updates the Sponsored Brands ads.

        Request Body (required)
        | **ads** ([{}]) :
            **adsId** (string) [required] : The product ad identifier.
            name (string) [optional] : The name of the ad
            state (State>String) [optional] : Entity state for create or update operation. Enum : [ENABLED, PAUSED]

        Returns
            ApiResponse

        """

        json_version = 'application/vnd.sbadresource.v' + str(version) + "+json"
        headers = {'Accept': json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/v4/ads/delete', method='POST')
    def delete_ads(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Deletes Sponsored Brands ads.

        Request Body
        | **adIdFilter** (dict) : Filter entities by the list of objectIds.
            include : list of adIds as string

        """

        json_version = 'application/vnd.sbadresource.v' + str(version) + "+json"
        headers = {'Accept': json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
