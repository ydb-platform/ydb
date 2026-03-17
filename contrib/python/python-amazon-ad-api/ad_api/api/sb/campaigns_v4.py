from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class CampaignsV4(Client):
    """
    Version 4 of the SB Campaigns
    """

    @sp_endpoint('/sb/v4/campaigns', method='POST')
    def create_campaigns(self, version: int = 4, **kwargs) -> ApiResponse:
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

        json_version = 'application/vnd.sbcampaignresource.v' + str(version) + "+json"

        headers = {
            "Accept": json_version,
        }

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/v4/campaigns', method='PUT')
    def edit_campaigns(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Update Sponsored Brand Campaigns

         Request body (required)
            | **campaignId** (string) : Entity object identifier.
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

        json_version = 'application/vnd.sbcampaignresource.v' + str(version) + "+json"
        headers = {"Accept": json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/v4/campaigns/delete', method='POST')
    @Utils.notsupported
    def delete_campaigns(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        The operation is currently not supported subject to potential change.
        Deletes Sponsored Brands campaigns.

        Request body (required)
            | **campaignIdFilter** (ObjectIdFilter): The identifier of an existing campaign. [required]
                | **include** (list>str): Entity object identifier. [required] minItems: 0 maxItems: 10

        Returns
            ApiResponse
        """

        json_version = 'application/vnd.sbcampaignresource.v' + str(version) + "+json"
        headers = {"Accept": json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/v4/campaigns/list', method='POST')
    def list_campaigns(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Lists Sponsored Brands campaigns.

        Request Body (optional) : Include the body for specific filtering, or leave empty to get all campaigns.
            | **start_index** (int): Sets a zero-based offset into the requested set of campaigns. Use in conjunction with the `count` parameter to control pagination of the returned array.. [optional] if omitted the server will use the default value of 0. Default value : 0
            | **state_filter** (State): The returned array is filtered to include only campaigns with state set to one of the values in the specified comma-delimited list. Defaults to `enabled` and `paused`.
                Note that Campaigns rejected during moderation have state set to `archived`. Available values : enabled, paused, archived[optional]
            | **name** (str): The returned array includes only campaigns with the specified name.. [optional]
            | **portfolio_id_filter** (str): The returned array includes only campaigns associated with Portfolio identifiers matching those specified in the comma-delimited string.. [optional]
            | **campaign_id_filter** (str): The returned array includes only campaigns with identifiers matching those specified in the comma-delimited string.. [optional]
            | **ad_format_filter** (AdFormat): The returned array includes only campaigns with ad format matching those specified in the comma-delimited adFormats. Returns all campaigns if not specified. Available values : productCollection, video[optional]
            | **max_results** (int) Number of records to include in the paginated response. Defaults to max page size for given API. Minimum 10 and a Maximum of 100 [optional]
            | **next_token** (string) Token value allowing to navigate to the next response page. [optional]
            | includeExtendedDataFields (boolean) Setting to true will slow down performance because the API needs to retrieve extra information for each campaign. [optional]

        Returns
            ApiResponse

        """

        json_version = 'application/vnd.sbcampaignresource.v' + str(version) + "+json"
        headers = {"Accept": json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
