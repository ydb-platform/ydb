from ad_api.base import Client, sp_endpoint, ApiResponse


class Brands(Client):
    """
    Use the Amazon Advertising API for Sponsored Brands for campaign, ad group, keyword, negative keyword, drafts, Stores, landing pages, and Brands management operations. For more information about Sponsored Brands, see the Sponsored Brands Support Center. For onboarding information, see the account setup topic.
    """

    @sp_endpoint('/brands', method='GET')
    def list_brands(self, **kwargs) -> ApiResponse:
        """
        Gets an array of Brand data objects for the Brand associated with the profile ID passed in the header. For more information about Brands, see [Brand Services](https://brandservices.amazon.com/).

        Keyword Args
            | None

        Returns:
            ApiResponse payload:

            | '**brandId**': *string*, {'description': 'The Brand identifier.'}
            | '**brandEntityId**': *string*, {'description': 'The Brand entity identifier.'}
            | '**brandRegistryName**': *string*, {'description': 'The Brand name.'}

        """
        return self._request(kwargs.pop('path'), params=kwargs)
