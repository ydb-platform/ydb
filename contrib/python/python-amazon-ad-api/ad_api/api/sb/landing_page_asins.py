from ad_api.base import Client, sp_endpoint, ApiResponse


class PageAsins(Client):
    """
    Use the Amazon Advertising API for Sponsored Brands for campaign, ad group, keyword, negative keyword, drafts, Stores, landing pages, and Brands management operations. For more information about Sponsored Brands, see the Sponsored Brands Support Center. For onboarding information, see the account setup topic.
    """

    @sp_endpoint('/pageAsins', method='GET')
    def get_page_asins(self, **kwargs) -> ApiResponse:
        """

        Gets ASIN information for a specified address.

        Keyword Args
            | query **pageUrl**:*string* | Required. For sellers, the address of a Store page. Vendors may also specify the address of a custom landing page. For more information, see the [Stores section](https://advertising.amazon.com/help#GPRM3ZHEXEY5RBFZ) of the Amazon Advertising support center.

        Returns
            asinList
        """
        return self._request(kwargs.pop('path'), params=kwargs)
