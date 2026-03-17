from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class Campaigns(Client):
    """
    Use the Amazon Advertising API for Sponsored Brands for campaign, ad group, keyword, negative keyword, drafts, Stores, landing pages, and Brands management operations. For more information about Sponsored Brands, see the Sponsored Brands Support Center. For onboarding information, see the account setup topic.
    """

    @sp_endpoint('/sb/campaigns', method='GET')
    @Utils.deprecated
    def list_campaigns(self, **kwargs) -> ApiResponse:
        """
        Gets an array of all campaigns associated with the client identifier passed in the authorization header, filtered by specified criteria.

        **Returns both productCollection and video campaigns. Use either 'adFormatFilter' or 'creativeType' to filter campaigns by ad formats (productCollection, video)**.

        Keyword Args
            | **start_index** (int): Sets a zero-based offset into the requested set of campaigns. Use in conjunction with the `count` parameter to control pagination of the returned array.. [optional] if omitted the server will use the default value of 0. Default value : 0
            | **count** (int): Sets the number of campaigns in the returned array. Use in conjunction with the `startIndex` parameter to control pagination. For example, to return the first ten campaigns set `startIndex=0` and `count=10`. To return the next ten campaigns, set `startIndex=10` and `count=10`, and so on. Default value : max page size[optional]
            | **state_filter** (State): The returned array is filtered to include only campaigns with state set to one of the values in the specified comma-delimited list. Defaults to `enabled` and `paused`.
                Note that Campaigns rejected during moderation have state set to `archived`. Available values : enabled, paused, archived[optional]
            | **name** (str): The returned array includes only campaigns with the specified name.. [optional]
            | **portfolio_id_filter** (str): The returned array includes only campaigns associated with Portfolio identifiers matching those specified in the comma-delimited string.. [optional]
            | **campaign_id_filter** (str): The returned array includes only campaigns with identifiers matching those specified in the comma-delimited string.. [optional]
            | **ad_format_filter** (AdFormat): The returned array includes only campaigns with ad format matching those specified in the comma-delimited adFormats. Returns all campaigns if not specified. Available values : productCollection, video[optional]
            | **creative_type** (CreativeType): Filter by the type of creative the campaign is associated with. To get non-video campaigns specify 'productCollection'. To get video campaigns, this must be set to 'video'. Returns all campaigns if not specified. Available values : productCollection, video[optional]

        :meta public:
        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sb/campaigns', method='POST')
    @Utils.deprecated
    def create_campaigns(self, **kwargs) -> ApiResponse:
        """
        Creates one or more new Campaigns.

        Request body
            | **name** (string): [optional] The name of the campaign. This name must be unique to the Amazon Advertising account to which the campaign is associated. Maximum length of the string is 128 characters.
            | **tags** (CampaignTags > string): [optional] A list of advertiser-specified custom identifiers for the campaign. Each customer identifier is a key-value pair. You can specify a maximum of 50 identifiers.
            | **budget** (float): [optional] The budget amount associated with the campaign.
            | **budget_type** (BudgetType > string) [optional] Note that for the lifetime budget type, `startDate` and `endDate` must be specified. The lifetime budget range is from 100 to 20,000,000 and daily budget range is 1 to 1,000,000 by default for most marketplaces. For the JP marketplace, the lifetime budget range is fromt 10,000 to 2,000,000,000, and the daily budget range is 100 to 21,000,000.., must be one of ["lifetime", "daily", ]
            | **start_date** (StartDate > string) [optional] The YYYYMMDD start date of the campaign. Must be equal to or greater than the current date. If this property is not included in the request, the startDate value is not updated. If set to null, startDate is set to the current date. [nullable: true] [pattern: ^\\d{8}$]
            | **end_date** (EndDate > string) [optional] The YYYYMMDD end date of the campaign. Must be greater than the value specified in the startDate field. If this property is not included in the request, the endDate value is not updated. If set to null, endDate is deleted from the draft campaign. [nullable: true] [pattern: ^\\d{8}$]
            | **ad_format** (AdFormat > string) [optional] The type of ad format. Enum: [ productCollection, video ]
            | **state** (State > string): [optional] Enum: [ enabled, paused, archived ]
            | **brand_entity_id** (str, writeOnly: true) [optional] The brand entity identifier. Note that this field is required for sellers. For more information, see the [Stores reference](https://advertising.amazon.com/API/docs/v2/reference/stores) or [Brands reference](https://advertising.amazon.com/API/docs/v3/reference/SponsoredBrands/Brands).
            | **bid_optimization** (bool) [optional] Set to `true` to allow Amazon to automatically optimize bids for placements below top of search if omitted the server will use the default value of True
            | **bid_multiplier** (float minimum: -99 maximum: 99) [optional] A bid multiplier. Note that this field can only be set when 'bidOptimization' is set to false. Value is a percentage to two decimal places. Example: If set to -40.00 for a $5.00 bid, the resulting bid is $3.00.
            | **portfolio_id** (int) [optional] The identifier of the portfolio to which the campaign is associated.
            | **creative** (SBCreative): [optional]
                | **brand_name** (str) A brand name. Maximum length is 30 characters.

                | **brand_logo_asset_id** (str) The identifier of the brand logo image from the Store assets library. See [listAssets](https://advertising.amazon.com/API/docs/v3/reference/SponsoredBrands/Stores) for more information. Note that for campaigns created in the Amazon Advertising console prior to release of the Store assets library, responses will not include a value for the brandLogoAssetID field.
                | **brand_logo_url** (str readOnly: true) The address of the hosted image.
                | **headline** (str) The headline text. Maximum length of the string is 50 characters for all marketplaces other than Japan, which has a maximum length of 35 characters.. [optional]
                | **asins** (str) An array of ASINs associated with the creative.

                    **NOTE**: do not pass an empty array, this results in an error.

                | **should_optimize_asins** (bool) default: false

                    **NOTE**: Starting on March 25th, 2021, this property will no longer be supported. This feature is currently available in the US and UK. Existing Sponsored Brands campaigns with product optimization enabled will no longer have the products in the creative automatically optimized. Campaigns with product optimization enabled will be converted to standard Sponsored Brands product collection campaigns with the default selected products showing in the creative. For POST and PUT operations, setting this property to `true` will not have any effect. The value returned in the response will always be `false`. For the GET operation, the value of this field will always be `false`. And starting on September 25th, 2021, this property will be removed completely. . [optional] if omitted the server will use the default value of False

            | **landing_page** (SBLandingPage): [optional]

                **asins** ([str]) An array of ASINs used to generate a simple landing page. The response includes the URL of the generated simple landing page. Do not include this property in the request if the `url` property is also included, these properties are mutually exclusive.

                **url** (str) URL of an existing simple landing page or Store page. Vendors may also specify the URL of a custom landing page. If a custom URL is specified, the landing page must include the ASINs of at least three products that are advertised as part of the campaign. Do not include this property in the request if the `asins` property is also included, these properties are mutually exclusive.

            | **keywords** ([SBCommonKeywordsKeywords]): An array of keywords associated with the campaign. [optional]

                **keyword_text** (str) The keyword text. Maximum of 10 words.. [optional]

                **native_language_keyword** (str) The unlocalized keyword text in the preferred locale of the advertiser.. [optional]

                **native_language_locale** (str) The locale preference of the advertiser. For example, if the advertiser’s preferred language is Simplified Chinese, set the locale to `zh_CN`. Supported locales include: Simplified Chinese (locale: zh_CN) for US, UK and CA. English (locale: en_GB) for DE, FR, IT and ES.. [optional]

                **match_type** (MatchType) [optional] The match type. For more information, see match types in the Amazon Advertising support center. Enum: [ broad, exact, phrase ]

                **bid** (float) The associated bid. Note that this value must be less than the budget associated with the Advertiser account. For more information, see [supported features](https://advertising.amazon.com/API/docs/v2/guides/supported_features) [optional]

            | **negative_keywords** ([SBUpdateDraftCampaignRequestWithKeywordsAllOfNegativeKeywords]): An array of negative keywords associated with the campaign.[optional]

                **keyword_text** (str): The keyword text. Maximum of 10 words. [optional]

                **match_type** (NegativeMatchType): [optional] The negative match type. For more information, see negative keyword match types in the Amazon Advertising support center. Enum:[ negativeExact, negativePhrase ]
        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sb/campaigns', method='PUT')
    @Utils.deprecated
    def edit_campaigns(self, **kwargs) -> ApiResponse:
        """
        Updates one or more campaigns.

        Mutable fields:
            | name
            | state
            | portfolioId
            | budget
            | bidOptimization
            | bidMultiplier
            | endDate

        Request body
            | **name** (string): [optional] The name of the campaign. This name must be unique to the Amazon Advertising account to which the campaign is associated. Maximum length of the string is 128 characters.
            | **state** (State > string): [optional] Enum: [ enabled, paused, archived ]
            | **portfolio_id** (int) [optional] The identifier of the portfolio to which the campaign is associated.
            | **budget** (float): [optional] The budget amount associated with the campaign.
            | **bid_optimization** (bool) [optional] Set to `true` to allow Amazon to automatically optimize bids for placements below top of search if omitted the server will use the default value of True
            | **bid_multiplier** (float minimum: -99 maximum: 99) [optional] A bid multiplier. Note that this field can only be set when 'bidOptimization' is set to false. Value is a percentage to two decimal places. Example: If set to -40.00 for a $5.00 bid, the resulting bid is $3.00.
            | **end_date** (EndDate > string) [optional] The YYYYMMDD end date of the campaign. Must be greater than the value specified in the startDate field. If this property is not included in the request, the endDate value is not updated. If set to null, endDate is deleted from the draft campaign. [nullable: true] [pattern: ^\\d{8}$]

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sb/campaigns/{}', method='GET')
    def get_campaign(self, campaignId, **kwargs) -> ApiResponse:
        """
        Gets a campaign specified by identifier.

        Keyword Args
            | query **campaignId** (integer): The identifier of an existing campaign. [required]
            | query **locale** (string): The returned array includes only keywords associated with locale matching those specified by identifier. [optional]




        """
        return self._request(fill_query_params(kwargs.pop('path'), campaignId), params=kwargs)

    @sp_endpoint('/sb/campaigns/{}', method='DELETE')
    def delete_campaign(self, campaignId, **kwargs) -> ApiResponse:
        """
        Archives a campaign specified by identifier.

        This operation is equivalent to an update operation that sets the status field to 'archived'. Note that setting the status field to 'archived' is permanent and can't be undone. See Developer Notes for more information

        Keyword Args
            | query **campaignId** (integer): The identifier of an existing campaign. [required]

        Returns
            ApiResponse


        """
        return self._request(fill_query_params(kwargs.pop('path'), campaignId), params=kwargs)
