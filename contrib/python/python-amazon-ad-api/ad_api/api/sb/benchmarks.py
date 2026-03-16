from ad_api.base import Client, sp_endpoint, ApiResponse, Utils, fill_query_params


class Benchmarks(Client):
    """
    Use the Amazon Advertising API for Sponsored Brands for campaign, ad group, keyword, negative keyword, drafts, Stores, landing pages, and Brands management operations. For more information about Sponsored Brands, see the Sponsored Brands Support Center. For onboarding information, see the account setup topic.
    """
    @sp_endpoint('/benchmarks/brands/{}/categories/{}', method='POST')
    def list_brand_and_category(self, brandName, categoryId, version: int = 1, **kwargs) -> ApiResponse:


        json_version = 'application/vnd.timeseriesdata.v' + str(version) + "+json"

        headers = {
            "Accept": json_version,
        }

        return self._request(fill_query_params(kwargs.pop('path'), brandName, categoryId), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)


    @sp_endpoint('/benchmarks/brands', method='GET')
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


    @sp_endpoint('/benchmarks/brandsAndCategories', method='POST')
    def list_brands_and_categories(self, version: int = 1, **kwargs) -> ApiResponse:
        """
        Creates Sponsored Brands campaigns.

        Request body (Required)
            Request body
            | **name** (string): [optional] The name of the campaign. This name must be unique to the Amazon Advertising account to which the campaign is associated. Maximum length of the string is 128 characters.
            | **state** (State > string): [optional] Enum: [ enabled, paused, archived ]
            | **portfolio_id** (int) [optional] The identifier of the portfolio to which the campaign is associated.
            | **budget** (float): [optional] The budget amount associated with the campaign.
            | **bid_optimization** (bool) [optional] Set to `true` to allow Amazon to automatically optimize bids for placements below top of search if omitted the server will use the default value of True
            | **bid_multiplier** (float minimum: -99 maximum: 99) [optional] A bid multiplier. Note that this field can only be set when 'bidOptimization' is set to false. Value is a percentage to two decimal places. Example: If set to -40.00 for a $5.00 bid, the resulting bid is $3.00.
            | **end_date** (EndDate > string) [optional] The YYYYMMDD end date of the campaign. Must be greater than the value specified in the startDate field. If this property is not included in the request, the endDate value is not updated. If set to null, endDate is deleted from the draft campaign. [nullable: true] [pattern: ^\\d{8}$]

        Returns
            ApiResponse
        """

        json_version = 'application/vnd.reportdata.v' + str(version) + "+json"

        headers = {
            "Accept": json_version,
        }

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
