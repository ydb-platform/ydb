from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class Stores(Client):
    """
    Use the Amazon Advertising API for Sponsored Brands for campaign, ad group, keyword, negative keyword, drafts, Stores, landing pages, and Brands management operations. For more information about Sponsored Brands, see the Sponsored Brands Support Center. For onboarding information, see the account setup topic.
    """

    @sp_endpoint('/stores/assets', method='GET')
    def list_assets(self, **kwargs) -> ApiResponse:
        """
        For sellers or vendors, gets an array of assets associated with the specified brand entity identifier. Vendors are not required to specify a brand entity identifier, and in this case all assets associated with the vendor are returned.

        Keyword Args
            | path **brandEntityId** (string): For sellers, this field is required. It is the Brand entity identifier of the Brand for which assets are returned. This identifier is retrieved using the getBrands operation. For vendors, this field is optional. If a vendor does not specify this field, all assets associated with the vendor are returned. For more information about the difference between a seller and a vendor, see the Amazon Advertising FAQ. [required]

            | query **mediaType** (string): Specifies the media types used to filter the returned array. Currently, only the brandLogo type is supported. If not specified, all media types are returned. Available values : brandLogo, image [optional]

        Returns:
            | ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path')), params=kwargs)

    @sp_endpoint('/v2/stores', method='GET')
    def list_stores(self, **kwargs) -> ApiResponse:
        """
        List store information for all registered stores under an advertiser.

        Keyword Args
            None

        """
        return self._request(fill_query_params(kwargs.pop('path')), params=kwargs)

    # It is not working the endpoint
    @sp_endpoint('/v2/stores/{}', method='GET')
    def get_store(self, brandEntityId, **kwargs) -> ApiResponse:
        return self._request(fill_query_params(kwargs.pop('path'), brandEntityId), params=kwargs)

    # It is not working Cannot consume content type
    @sp_endpoint('/stores/assets', method='POST')
    def create_asset(self, contentDisposition, contentType, **kwargs) -> ApiResponse:
        headers = {'Content-Disposition': contentDisposition, 'Content-Type': contentType}
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs, headers=headers)
