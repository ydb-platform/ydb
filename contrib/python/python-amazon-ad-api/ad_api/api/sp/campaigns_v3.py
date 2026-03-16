from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class CampaignsV3(Client):
    r"""
    Amazon Ads API - Sponsored Products
    """

    @sp_endpoint('/sp/campaigns', method='POST')
    def create_campaigns(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        create_campaigns(body: (dict, str, list)) -> ApiResponse

        Request Body [Required]
            | **name** (string): [required] The name of the campaign. This name must be unique to the Amazon Advertising account to which the campaign is associated. Maximum length of the string is 128 characters.
            | **state** (State > string): [required] Enum: [ enabled, paused, archived ]
            | **portfolio_id** (int) [optional] The identifier of the portfolio to which the campaign is associated.
            | **budget** (float): [required]
                | **budgetType** (BudgetType > String) : Enum [DAILY]
                | **budget** (float) : The budget amount associated with the campaign.
            | **targetingType** (Targeting > string) : [required] Enum : [AUTO, MANUAL]
            | **dynamicBidding** ({}) [optional]
                | **placementBidding** (string) : You can enable controls to adjust your bid based on the placement location. Specify a location where you want to use bid controls.
                | **strategy** (BiddingStrategy > String) : Enum [LEGACY_FOR_SALES, AUTO_FOR_SALES, MANUAL, RULE_BASED]
            | **startDate** [optional] (string) : A starting date for the campaign to go live. The format of the date is YYYYMMDD.
            | **endDate** [optional] (string) : An ending date for the campaign to stop running. The format of the date is YYYYMMDD.
            | **tags** ({string}) : A list of advertiser-specified custom identifiers for the campaign.

        Returns
            ApiResponse

        """

        schema_version = 'application/vnd.spCampaign.v' + str(version) + '+json'
        headers = {"Accept": schema_version, "Content-Type": schema_version}
        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/campaigns', method='PUT')
    def edit_campaigns(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        edit_campaigns(body: (dict, str, list)) -> ApiResponse

        Update existing Sponsored Product Campaigns.

        Request Body [Required]
            | **name** (string): [optional] The name of the campaign. This name must be unique to the Amazon Advertising account to which the campaign is associated. Maximum length of the string is 128 characters.
            | **state** (State > string): [optional] Enum: [ enabled, paused, archived ]
            | **portfolio_id** (int) [optional] The identifier of the portfolio to which the campaign is associated.
            | **budget** (float): [required]
                | **budgetType** (BudgetType > String) : Enum [DAILY]
                | **budget** (float) : The budget amount associated with the campaign.
            | **targetingType** (Targeting > string) : [optional] Enum : [AUTO, MANUAL]
            | **dynamicBidding** ({}) [optional]
                | **placementBidding** (string) : You can enable controls to adjust your bid based on the placement location. Specify a location where you want to use bid controls.
                | **strategy** (BiddingStrategy > String) [required] : Enum [LEGACY_FOR_SALES, AUTO_FOR_SALES, MANUAL, RULE_BASED]
            | **campaignId** (string) : the entity object identifier
            | **startDate** [optional] (string) : A starting date for the campaign to go live. The format of the date is YYYYMMDD.
            | **endDate** [optional] (string) : An ending date for the campaign to stop running. The format of the date is YYYYMMDD.
            | **tags** ({string}) : A list of advertiser-specified custom identifiers for the campaign.

        Returns
            ApiResponse
        """

        schema_version = 'application/vnd.spCampaign.v' + str(version) + '+json'
        headers = {"Accept": schema_version, "Content-Type": schema_version}
        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/campaigns/list', method='POST')
    def list_campaigns(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        list_campaigns(body: (dict, str, list)) -> ApiResponse

        Request Body (optional) : Include the body for specific filtering, or leave empty to get all campaigns.
            | **state_filter** (State): The returned array is filtered to include only campaigns with state set to one of the values in the specified comma-delimited list. Defaults to `enabled` and `paused`.
                Note that Campaigns rejected during moderation have state set to `archived`. Available values : enabled, paused, archived[optional]
            | **nameFilter** (str): The returned array includes only campaigns with the specified name.. [optional]
                | queryTermMatchType (MatchType > string) Enum : [ BROAD_MATCH, EXACT_MATCH ]
            | **portfolio_id_filter** (str): The returned array includes only campaigns associated with Portfolio identifiers matching those specified in the comma-delimited string.. [optional]
            | **campaign_id_filter** (str): The returned array includes only campaigns with identifiers matching those specified in the comma-delimited string.. [optional]
            | **ad_format_filter** (AdFormat): The returned array includes only campaigns with ad format matching those specified in the comma-delimited adFormats. Returns all campaigns if not specified. Available values : productCollection, video[optional]
            | **max_results** (int) Number of records to include in the paginated response. Defaults to max page size for given API. Minimum 10 and a Maximum of 100 [optional]
            | **next_token** (string) Token value allowing to navigate to the next response page. [optional]
            | includeExtendedDataFields (boolean) Setting to true will slow down performance because the API needs to retrieve extra information for each campaign. [optional]

        Returns
            ApiResponse

        """
        schema_version = 'application/vnd.spCampaign.v' + str(version) + '+json'
        headers = {"Accept": schema_version, "Content-Type": schema_version}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/campaigns/delete', method='POST')
    def delete_campaigns(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        delete_campaigns(body: (dict, str, list)) -> ApiResponse

        Deletes Sponsored Products campaigns.

        Request body (required)
            | **campaignIdFilter** (ObjectIdFilter): The identifier of an existing campaign. [required]
                | **include** (list>str): Entity object identifier. [required] minItems: 0 maxItems: 1000

        Returns
            ApiResponse

        """
        schema_version = 'application/vnd.spCampaign.v' + str(version) + '+json'
        headers = {"Accept": schema_version, "Content-Type": schema_version}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
