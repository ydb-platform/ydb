from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class AdGroupsV3(Client):
    """
    Version 3 of Sponsored Products API
    """

    @sp_endpoint("/sp/adGroups", method="POST")
    def create_ad_groups(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Creates one or more ad groups.

        Request body: (required) An array of ad groups
            | **name** (*string*) : A name for the ad group
            | **campaignId**: (*string*) : An existing campaign to which the ad group is associated.
            | **defaultBid**: (*float*) A bid value for use when no bid is specified for keywords in the ad group
            | **state**: *string* A name for the ad group, Enum: [ enabled, paused, archived ]

        Returns:
            ApiResponse
        """

        json_version = "application/vnd.spAdGroup.v" + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint("/sp/adGroups", method="PUT")
    def edit_ad_groups(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Creates one or more ad groups.

        Request body: (required) An array of ad groups
            | **name** (*string*) : A name for the ad group
            | **adGroupId** (*string*) : The identifier of the ad group.
            | **defaultBid**: (*float*) A bid value for use when no bid is specified for keywords in the ad group
            | **state**: *string* A name for the ad group, Enum: [ enabled, paused, archived ]

        Returns:
            ApiResponse
        """

        json_version = "application/vnd.spAdGroup.v" + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint("/sp/adGroups/delete", method="POST")
    def delete_ad_groups(self, version: int = 3, **kwargs) -> ApiResponse:
        """
        Deletes Sponsored Products ad groups by changing it's state to "ARCHIVED".

        Request body (required)
            | **adGroupIdFilter** (ObjectIdFilter): The identifier of an existing ad group. [required]
                | **include** (list>str): Entity object identifier. [required] minItems: 0 maxItems: 1000

        Returns
            ApiResponse
        """

        json_version = "application/vnd.spAdGroup.v" + str(version) + "+json"

        headers = {"Accept": json_version, "Content-Type": json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint("/sp/adGroups/list", method="POST")
    def list_ad_groups(self, version: int = 3, **kwargs) -> ApiResponse:
        """
        Lists Sponsored Products campaigns.

        Request Body (optional) : Include the body for specific filtering, or leave empty to get all ad groups.
            | **state_filter** (State): The returned array is filtered to include only campaigns with state set to one of the values in the specified comma-delimited list. Defaults to `enabled` and `paused`.
                Note that Campaigns rejected during moderation have state set to `archived`. Available values : enabled, paused, archived[optional]
            | **nameFilter** (str): The returned array includes only campaigns with the specified name.. [optional]
                | queryTermMatchType (MatchType > string) Enum : [ BROAD_MATCH, EXACT_MATCH ]
            | **portfolio_id_filter** (str): The returned array includes only campaigns associated with Portfolio identifiers matching those specified in the comma-delimited string.. [optional]
            | **campaign_id_filter** (str): The returned array includes only campaigns with identifiers matching those specified in the comma-delimited string.. [optional]
            | **includeExtendedDataFields** (boolean) Whether to get entity with extended data fields such as creationDate, lastUpdateDate, servingStatus [optional]
            | **max_results** (int) Number of records to include in the paginated response. Defaults to max page size for given API. Minimum 10 and a Maximum of 100 [optional]
            | **next_token** (string) Token value allowing to navigate to the next response page. [optional]
            | **campaignTargetingTypeFilter** (TargetingType > String) Enum : [ AUTO, MANUAL ]

        Returns
            ApiResponse
        """

        json_version = "application/vnd.spAdGroup.v" + str(version) + "+json"

        headers = {"Accept": json_version, "Content-Type": json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
