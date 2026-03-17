from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class AdGroupsV4(Client):
    """
    Version 4 of Sponsored Brands
    """

    @sp_endpoint('/sb/v4/adGroups', method='POST')
    def create_ad_groups(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Creates Sponsored Brand Ad Group.

        Request Body
        | campaignId (string) : The identifier of the campaign to which the keyword is associated.
        | name (string) : The name of the ad group.
        | state (CreateOrUpdateEntityState > string) : Entity state for create or update operation. Enum : [ENABLED, PAUSED]

        Returns
            ApiResponse

        """

        json_ressource = 'application/vnd.sbadgroupresource.v' + str(version) + "+json"
        headers = {'Accept': json_ressource}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/v4/adGroups', method='PUT')
    def update_ad_groups(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Update Sponsored Brand Ad groups.

         Request Body
        | campaignId (string) : The identifier of the campaign to which the keyword is associated. [optional]
        | name (string) : The name of the ad group. [optional]
        | state (CreateOrUpdateEntityState > string) : Entity state for create or update operation. Enum : [ENABLED, PAUSED]

        Returns
            ApiResponse

        """

        json_ressource = 'application/vnd.sbadgroupresource.v' + str(version) + "+json"
        headers = {'Accept': json_ressource}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sb/v4/adGroups/list', method='POST')
    def list_ad_groups(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        List Sponsored Brand Ad groups.

        Request Body (optional)
        | **campaignIdFilter** (dict) : Filter entities by the list of objectIds.
        | **stateFilter** (dict) : Filter entities by state.
        | **maxResults** (int) : Number of records to include in the paginated response. Defaults to max page size for given API.
        | **nextToken** (string) : Token value allowing to navigate to the next response page.
        | **adGroupIdFilter** (dict) : Filter entities by the list of objectIds.
        | **includeExtendedDataFields** (boolean) Setting to true will slow down performance because the API needs to retrieve extra information for each campaign.
        | **nameFilter** (dict) : Filter entities by name.

         Returns:
            | ApiResponse
        """

        json_ressource = 'application/vnd.sbadgroupresource.v' + str(version) + "+json"
        headers = {'Accept': json_ressource}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint("/sb/v4/adGroups/delete", method="POST")
    def delete_ad_groups(self, version: int = 4, **kwargs) -> ApiResponse:
        """
        Delete Sponsored Brands ad groups.

        Request Body (optional) :
            **adGroupIdFilter** (dict) : Filter entities by the list of objectIds. [optional]
                include (list) : Entity object identifier.

        Returns
            ApiResponse
        """

        json_ressource = 'application/vnd.sbadgroupresource.v' + str(version) + "+json"
        headers = {'Accept': json_ressource}

        return self._request(kwargs.pop('path'), Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
