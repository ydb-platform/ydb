from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class Campaigns(Client):
    @sp_endpoint('/sd/campaigns', method='GET')
    def list_campaigns(self, **kwargs) -> ApiResponse:
        r"""
        list_campaigns(self, **kwargs) -> ApiResponse

        Gets an array of campaigns.

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.

            query **name**:*string* | Optional. Restricts results to campaigns with the specified name.

            query **portfolioIdFilter**:*string* | Optional. A comma-delimited list of portfolio identifiers.

            query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sd/campaigns', method='PUT')
    def edit_campaigns(self, **kwargs) -> ApiResponse:
        r"""
        edit_campaigns(self, **kwargs) -> ApiResponse

        Updates one or more campaigns.

        body: | REQUIRED {'description': 'An array of ad groups.}'

            | '**campaignId**': *number*, {'description': 'The identifier of an existing campaign to update.'}
            | '**portfolioId**': *number*, {'description': 'The identifier of an existing portfolio to which the campaign is associated'}
            | '**name**': *string*, {'description': 'The name for the campaign'}
            | '**tags**': *CampaignTags*, {'description': 'A list of advertiser-specified custom identifiers for the campaign. Each customer identifier is a key-value pair. You can specify a maximum of 50 identifiers.'}
            | '**state**': *string*, {'description': 'The current resource state.', 'Enum': '[ enabled, paused, archived ]'}
            | '**dailyBudget**': *number($float)*, {'description': 'The daily budget for the campaign.'}
            | '**startDate**': *string*, {'description': 'The starting date for the campaign to go live. The format of the date is YYYYMMDD.'}
            | '**endDate**': *string* nullable: true, {'description': 'The ending date for the campaign to stop running. The format of the date is YYYYMMDD.'}
            | '**premiumBidAdjustment**': *boolean*, {'description': 'If set to true, Amazon increases the default bid for ads that are eligible to appear in this placement. See developer notes for more information.'}
            | '**bidding**': *Bidding*, {'strategy': 'string', 'Enum': '[ legacyForSales, autoForSales, manual ]', 'adjustments': '{...}'}

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sd/campaigns', method='POST')
    def create_campaigns(self, **kwargs) -> ApiResponse:
        r"""
        create_campaigns(self, **kwargs) -> ApiResponse

        Creates one or more campaigns.

        body: | REQUIRED {'description': 'An array of ad groups.}'

            | '**portfolioId**': *number*, {'description': 'The identifier of an existing portfolio to which the campaign is associated'}
            | '**name**': *string*, {'description': 'A name for the campaign'}
            | '**tags**': *string*, {'description': 'A list of advertiser-specified custom identifiers for the campaign. Each customer identifier is a key-value pair. You can specify a maximum of 50 identifiers.'}
            | '**campaignType**': *string*, {'description': 'The advertising product managed by this campaign', 'Enum': '[ sponsoredProducts ]'}
            | '**targetingType**': *string*, {'description': 'The type of targeting for the campaign.', 'Enum': '[ manual, auto ]'}
            | '**state**': *string*, {'description': 'The current resource state.', 'Enum': '[ enabled, paused, archived ]'}
            | '**dailyBudget**': *number($float)*, {'description': 'A daily budget for the campaign.'}
            | '**startDate**': *string*, {'description': 'A starting date for the campaign to go live. The format of the date is YYYYMMDD.'}
            | '**endDate**': *string* nullable: true, {'description': 'An ending date for the campaign to stop running. The format of the date is YYYYMMDD.'}
            | '**premiumBidAdjustment**': *boolean*, {'description': 'If set to true, Amazon increases the default bid for ads that are eligible to appear in this placement. See developer notes for more information.'}
            | '**bidding**': *Bidding*, {'strategy': 'string', 'Enum': '[ legacyForSales, autoForSales, manual ]', 'adjustments': '{...}'}

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sd/campaigns/{}', method='GET')
    def get_campaign(self, campaignId, **kwargs) -> ApiResponse:
        r"""

        get_campaign(self, campaignId, **kwargs) -> ApiResponse

        Gets a campaign specified by identifier.

            path **campaignId**:*number* | Required. The identifier of an existing campaign.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), campaignId), params=kwargs)

    @sp_endpoint('/sd/campaigns/{}', method='DELETE')
    def delete_campaign(self, campaignId, **kwargs) -> ApiResponse:
        r"""

        delete_campaign(self, campaignId, **kwargs) -> ApiResponse

        Sets the campaign status to archived. Archived entities cannot be made active again. See developer notes for more information.

            path **campaignId**:*number* | Required. The identifier of an existing campaign.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), campaignId), params=kwargs)

    @sp_endpoint('/sd/campaigns/extended', method='GET')
    def list_campaigns_extended(self, **kwargs) -> ApiResponse:
        r"""
        list_campaigns_extended(self, **kwargs) -> ApiResponse

        Gets an array of campaigns with extended data fields.

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.

            query **name**:*string* | Optional. Restricts results to campaigns with the specified name.

            query **portfolioIdFilter**:*string* | Optional. A comma-delimited list of portfolio identifiers.

            query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sd/campaigns/extended/{}', method='GET')
    def get_campaign_extended(self, campaignId, **kwargs) -> ApiResponse:
        r"""

        get_campaign_extended(self, campaignId, **kwargs) -> ApiResponse

        Gets an array of campaigns with extended data fields.

            path **campaignId**:*number* | Required. The identifier of an existing campaign.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), campaignId), params=kwargs)
