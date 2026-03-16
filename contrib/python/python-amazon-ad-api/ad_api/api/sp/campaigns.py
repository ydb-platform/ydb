from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils
import json


class Campaigns(Client):
    r"""
    Campaigns(account='default', marketplace: Marketplaces = Marketplaces.EU, credentials=None, debug=False)

    Amazon Ads API - Sponsored Products

    """

    @sp_endpoint('/v2/sp/campaigns', method='PUT')
    @Utils.deprecated
    def edit_single_campaign_assistant(
        self,
        campaign_id: int,
        portfolio_id: int = None,
        campaign_name: str = None,
        po_number: str = None,
        account_manager: str = None,
        campaign_status: str = None,
        daily_budget: int = None,
        start_date: str = None,
        end_date: str = None,
        premium_bid_adjustment: bool = None,
        strategy: str = None,
        predicate: str or tuple = None,
        percentage: str or tuple = None,
        **kwargs,
    ) -> ApiResponse:
        r"""
        edit_single_campaign_assistant(campaign_id: int, portfolio_id: int = None, campaign_name: str, po_number: str = None, account_manager: str = None, campaign_status: str = None, daily_budget: int, start_date: str, end_date: str = None, premium_bid_adjustment: bool = None, strategy:str = None, predicate:str or tuple = None, percentage:int or tuple = None, **kwargs) -> ApiResponse

        Edit one campaigns and create the body based on the params provided, at least one of the optional params need to be set or a INVALID_ARGUMENT code is thorwn

        Kwargs:
            | **campaign_id** (number): [required] The identifier of an existing campaign to update.
            | **portfolio_id** (number): [optional] The identifier of an existing portfolio to which the campaign is associated
            | **campaign_name** (string): [optional] A name for the campaign
            | **po_number** (string): [optional] 'A list of advertiser-specified custom identifiers for the campaign. Each customer identifier is a key-value pair. You can specify a maximum of 50 identifiers
            | **account_manager** (string): [optional] A list of advertiser-specified custom identifiers for the campaign. Each customer identifier is a key-value pair. You can specify a maximum of 50 identifiers
            | **campaign_status**': (string): [optional] The current resource state Values: enabled, paused, archived. Default: enabled
            | **daily_budget** (float): [optional] A daily budget for the campaign
            | **start_date** (string): [optional] A starting date for the campaign to go live. The format of the date is YYYYMMDD
            | **end_date** (string): [optional] An ending date for the campaign to stop running. The format of the date is YYYYMMDD
            | **premium_bid_adjustment**' (boolean): [optional] If set to true, Amazon increases the default bid for ads that are eligible to appear in this placement. See developer notes for more information. Tip: When Campaign has been adopted to enhanced bidding premiumBidAdjustment can not be set
            | **strategy** (string): [optional] The bidding strategy. 'Values': legacyForSales (Dynamic bids - down only), autoForSales (Dynamic bids - up and down), manual (Fixed bid)
            | **predicate** (string or tuple(str)): [optional] You can enable controls to adjust your bid based on the placement location. Specify a location where you want to use bid controls. The percentage value set is the percentage of the original bid for which you want to have your bid adjustment increased. For example, a 50% adjustment on a $1.00 bid would increase the bid to $1.50 for the opportunity to win a specified placement. 'Values':  placementTop (str), placementProductPage (str), ("placementTop", "placementProductPage") (tuple)
            | **percentage** (float or tuple(float)): [optional] The bid adjustment percentage value. Example: 15 (float), (15, 25) (tuple)

        Returns:

            ApiResponse

        """

        optional = {}

        if portfolio_id is not None:
            optional.update({'portfolioId': portfolio_id})

        if campaign_name is not None:
            optional.update({'name': campaign_name})

        if po_number is not None and account_manager is not None:
            optional.update({"tags": {'PONumber': po_number, 'accountManager': account_manager}})

        if campaign_status is not None:
            optional.update({'state': campaign_status})

        if daily_budget is not None:
            optional.update({'dailyBudget': daily_budget})

        if start_date is not None:
            optional.update({'startDate': start_date})

        if end_date is not None:
            optional.update({'endDate': end_date})

        if premium_bid_adjustment is not None:
            optional.update({'premiumBidAdjustment': premium_bid_adjustment})

        if strategy is not None and predicate is None and percentage is None:
            optional.update({'bidding': {'strategy': strategy, 'adjustments': []}})

        if strategy is not None and predicate is not None and percentage is not None:
            if isinstance(predicate, str):
                optional.update(
                    {
                        'bidding': {
                            'strategy': strategy,
                            'adjustments': [{'predicate': predicate, 'percentage': percentage}],
                        }
                    }
                )

            if isinstance(predicate, tuple) and len(predicate) == 2 and len(percentage) == 2:
                options = []
                for i in range(len(predicate)):
                    option = {"predicate": predicate[i], "percentage": percentage[i]}
                    options.append(option)

                bidding = {'strategy': strategy, 'adjustments': options}

                dictionary = {}
                dictionary["bidding"] = bidding

                optional.update(dictionary)

        required = {
            'campaignId': campaign_id,
        }

        if not optional:
            description = 'No changes submitted for entity %s' % campaign_id
            obj = [{'code': 'INVALID_ARGUMENT', 'description': description}]
            return ApiResponse(obj)
        else:
            required.update(optional)
            data_str = json.dumps([required])
            return self._request(kwargs.pop('path'), data=data_str, params=kwargs)

    @sp_endpoint('/v2/sp/campaigns', method='POST')
    @Utils.deprecated
    def create_single_campaign_assistant(
        self,
        campaign_name: str,
        targeting_type: str,
        daily_budget: int,
        start_date: str,
        end_date: str = None,
        campaign_status: str = "enabled",
        portfolio_id: int = None,
        po_number: str = None,
        account_manager: str = None,
        premium_bid_adjustment: bool = None,
        strategy: str = None,
        predicate: str or tuple = None,
        percentage: int or tuple = None,
        **kwargs,
    ) -> ApiResponse:
        r"""
        create_single_campaign_assistant(campaign_name: str,targeting_type: str, daily_budget: int, start_date: str, end_date: str = None, campaign_status: str = "enabled", portfolio_id: int = None, po_number: str = None, account_manager: str = None, premium_bid_adjustment: bool = False, strategy:str = None, predicate:str = None, percentage:int = None, **kwargs) -> ApiResponse

        Creates one campaigns and create the body based on the params provided

        Kwargs:

            | **campaign_name** (string): [required] A name for the campaign
            | **campaign_type** (string): [fixed] The advertising product managed by this campaign. Value: sponsoredProducts
            | **targeting_type** (string): [required] The type of targeting for the campaign. Values: manual, auto
            | **daily_budget** (float): [required] A daily budget for the campaign
            | **start_date** (string): [required] A starting date for the campaign to go live. The format of the date is YYYYMMDD
            | **end_date** (string): [optional] An ending date for the campaign to stop running. The format of the date is YYYYMMDD
            | **campaign_status**': (string): [optional] The current resource state Values: enabled, paused, archived. Default: enabled
            | **portfolio_id** (number): [optional] The identifier of an existing portfolio to which the campaign is associated
            | **po_number** (string): [optional] 'A list of advertiser-specified custom identifiers for the campaign. Each customer identifier is a key-value pair. You can specify a maximum of 50 identifiers
            | **account_manager** (string): [optional] A list of advertiser-specified custom identifiers for the campaign. Each customer identifier is a key-value pair. You can specify a maximum of 50 identifiers
            | **premium_bid_adjustment**' (boolean) If set to true, Amazon increases the default bid for ads that are eligible to appear in this placement. See developer notes for more information.
            | **strategy** (string): [optional] The bidding strategy. 'Values': legacyForSales (Dynamic bids - down only), autoForSales (Dynamic bids - up and down), manual (Fixed bid)
            | **predicate** (string): [optional] You can enable controls to adjust your bid based on the placement location. Specify a location where you want to use bid controls. The percentage value set is the percentage of the original bid for which you want to have your bid adjustment increased. For example, a 50% adjustment on a $1.00 bid would increase the bid to $1.50 for the opportunity to win a specified placement. 'Values':  placementTop, placementProductPage
            | **percentage** (float): [optional] The bid adjustment percentage value.

        Returns:

            ApiResponse

        """

        optional = {}

        if portfolio_id is not None:
            optional.update({'portfolioId': portfolio_id})

        if po_number is not None and account_manager is not None:
            optional.update({"tags": {'PONumber': po_number, 'accountManager': account_manager}})

        if end_date is not None:
            optional.update({'endDate': end_date})

        if premium_bid_adjustment is not None:
            optional.update({'premiumBidAdjustment': premium_bid_adjustment})

        if strategy is not None and predicate is not None and percentage is not None:
            if isinstance(predicate, str):
                optional.update(
                    {
                        'bidding': {
                            'strategy': strategy,
                            'adjustments': [{'predicate': predicate, 'percentage': percentage}],
                        }
                    }
                )

            if isinstance(predicate, tuple) and len(predicate) == 2 and len(percentage) == 2:
                options = []
                for i in range(len(predicate)):
                    option = {"predicate": predicate[i], "percentage": percentage[i]}
                    options.append(option)

                bidding = {'strategy': strategy, 'adjustments': options}

                dictionary = {}
                dictionary["bidding"] = bidding

                optional.update(dictionary)

        required = {
            'name': campaign_name,
            'campaignType': 'sponsoredProducts',
            'targetingType': targeting_type,
            'state': campaign_status,
            'dailyBudget': daily_budget,
            'startDate': start_date,
        }

        if optional:
            required.update(optional)

        data_str = json.dumps([required])
        return self._request(kwargs.pop('path'), data=data_str, params=kwargs)

    @sp_endpoint('/v2/sp/campaigns', method='POST')
    @Utils.deprecated
    def create_campaigns(self, **kwargs) -> ApiResponse:
        r"""
        create_campaigns(body: (dict, str, list)) -> ApiResponse

        Creates one or more campaigns.

        body: | REQUIRED {'description': 'An array of campaigns.}'

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
        body = Utils.convert_body(kwargs.pop('body'))
        return self._request(kwargs.pop('path'), data=body, params=kwargs)

    @sp_endpoint('/v2/sp/campaigns', method='PUT')
    @Utils.deprecated
    def edit_campaigns(self, **kwargs) -> ApiResponse:
        r"""
        edit_campaigns(body: (dict, str, list)) -> ApiResponse

        Updates one or more campaigns.

        body: | REQUIRED {'description': 'An array of campaigns.}'

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
        body = Utils.convert_body(kwargs.pop('body'))
        return self._request(kwargs.pop('path'), data=body, params=kwargs)

    @sp_endpoint('/v2/sp/campaigns', method='GET')
    @Utils.deprecated
    def list_campaigns(self, **kwargs) -> ApiResponse:
        r"""
        list_campaigns(**kwargs) -> ApiResponse

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

    @sp_endpoint('/v2/sp/campaigns/{}', method='GET')
    @Utils.deprecated
    def get_campaign(self, campaignId, **kwargs) -> ApiResponse:
        r"""

        get_campaign(campaignId: int) -> ApiResponse

        Gets a campaign specified by identifier.

            path **campaignId**:*number* | Required. The identifier of an existing campaign.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), campaignId), params=kwargs)

    @sp_endpoint('/v2/sp/campaigns/{}', method='DELETE')
    @Utils.deprecated
    def delete_campaign(self, campaignId, **kwargs) -> ApiResponse:
        r"""

        delete_campaign(campaignId: int) -> ApiResponse

        Sets the campaign status to archived. Archived entities cannot be made active again. See developer notes for more information.

            path **campaignId**:*number* | Required. The identifier of an existing campaign.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), campaignId), params=kwargs)

    @sp_endpoint('/v2/sp/campaigns/extended', method='GET')
    @Utils.deprecated
    def list_campaigns_extended(self, **kwargs) -> ApiResponse:
        r"""
        list_campaigns_extended(**kwargs) -> ApiResponse

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

    @sp_endpoint('/v2/sp/campaigns/extended/{}', method='GET')
    @Utils.deprecated
    def get_campaign_extended(self, campaignId, **kwargs) -> ApiResponse:
        r"""
        get_campaign_extended(campaignId: int) -> ApiResponse

        Gets an array of campaigns with extended data fields.

            path **campaignId**:*number* | Required. The identifier of an existing campaign.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), campaignId), params=kwargs)
