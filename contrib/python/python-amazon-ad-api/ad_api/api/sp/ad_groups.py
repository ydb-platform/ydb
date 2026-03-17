from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class AdGroups(Client):
    @sp_endpoint('/v2/sp/adGroups', method='POST')
    @Utils.deprecated
    def create_ad_groups(self, **kwargs) -> ApiResponse:
        r"""
        create_ad_groups(self, \*\*kwargs) -> ApiResponse

        Creates one or more ad groups.

        body: | REQUIRED {'description': 'An array of ad groups.}'

            | '**name**': *string*, {'description': 'A name for the ad group'}
            | '**campaignId**': *number*, {'description': 'An existing campaign to which the ad group is associated'}
            | '**defaultBid**': *number($float)*, {'description': 'A bid value for use when no bid is specified for keywords in the ad group', 'minimum': '0.02'}
            | '**state**': *string*, {'description': 'A name for the ad group', 'Enum': '[ enabled, paused, archived ]'}


        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/v2/sp/adGroups', method='PUT')
    @Utils.deprecated
    def edit_ad_groups(self, **kwargs) -> ApiResponse:
        r"""
        edit_ad_group(self, \*\*kwargs) -> ApiResponse

        Updates one or more ad groups.

        body: | REQUIRED {'description': 'An array of ad groups.}'

            | '**adGroupId**': *number*, {'description': 'The identifier of the ad group.'}
            | '**name**': *string*, {'description': 'The name of the ad group.'}
            | '**defaultBid**': *number($float)*, {'description': 'The bid value used when no bid is specified for keywords in the ad group.', 'minimum': '0.02'}
            | '**state**': *string*, {'description': 'The current resource state', 'Enum': '[ enabled, paused, archived ]'}

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/v2/sp/adGroups', method='GET')
    @Utils.deprecated
    def list_ad_groups(self, **kwargs) -> ApiResponse:
        r"""
        list_ad_groups(self, \*\*kwargs) -> ApiResponse

        Gets an array of AdGroup objects for a requested set of Sponsored Display ad groups. Note that the AdGroup object is designed for performance, and includes a small set of commonly used fields to reduce size. If the extended set of fields is required, use the ad group operations that return the AdGroupResponseEx object.

            query **startIndex**:*integer* | Optional. Sets a cursor into the requested set of campaigns. Use in conjunction with the count parameter to control pagination of the returned array. 0-indexed record offset for the result set, defaults to 0.

            query **count**:*integer* | Optional. Sets the number of AdGroup objects in the returned array. Use in conjunction with the startIndex parameter to control pagination. For example, to return the first ten ad groups set startIndex=0 and count=10. To return the next ten ad groups, set startIndex=10 and count=10, and so on. Defaults to max page size.

            query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived

            query **campaignIdFilter**:*string* | Optional. The returned array is filtered to include only ad groups associated with the campaign identifiers in the specified comma-delimited list.

            query **adGroupIdFilter**:*string* | Optional. The returned array is filtered to include only ad groups with an identifier specified in the comma-delimited list.

            query **name**:*string* | Optional. The returned array includes only ad groups with the specified name.



        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/v2/sp/adGroups/{}', method='GET')
    @Utils.deprecated
    def get_ad_group(self, adGroupId, **kwargs) -> ApiResponse:
        r"""

        get_ad_group(self, adGroupId, \*\*kwargs) -> ApiResponse

        Gets an ad group specified by identifier.

            path **adGroupId**:*number* | Required. The identifier of an existing ad group.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), adGroupId), params=kwargs)

    @sp_endpoint('/v2/sp/adGroups/{}', method='DELETE')
    @Utils.deprecated
    def delete_ad_group(self, adGroupId, **kwargs) -> ApiResponse:
        r"""

        delete_ad_group(self, adGroupId, \*\*kwargs) -> ApiResponse

        Sets the ad group status to archived. Archived entities cannot be made active again. See developer notes for more information.

            path **adGroupId**:*number* | Required. The identifier of an existing ad group.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), adGroupId), params=kwargs)

    @sp_endpoint('/v2/sp/adGroups/extended', method='GET')
    @Utils.deprecated
    def list_ad_groups_extended(self, **kwargs) -> ApiResponse:
        r"""
        list_ad_groups_extended(self, \*\*kwargs) -> ApiResponse

        Gets an array of AdGroup objects for a requested set of Sponsored Display ad groups. Note that the AdGroup object is designed for performance, and includes a small set of commonly used fields to reduce size. If the extended set of fields is required, use the ad group operations that return the AdGroupResponseEx object.

            query: **startIndex**:*integer* | Optional. Sets a cursor into the requested set of campaigns. Use in conjunction with the count parameter to control pagination of the returned array. 0-indexed record offset for the result set, defaults to 0.

            query **count**:*integer* | Optional. Sets the number of AdGroup objects in the returned array. Use in conjunction with the startIndex parameter to control pagination. For example, to return the first ten ad groups set startIndex=0 and count=10. To return the next ten ad groups, set startIndex=10 and count=10, and so on. Defaults to max page size.

            query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived

            query **campaignIdFilter**:*string* | Optional. The returned array is filtered to include only ad groups associated with the campaign identifiers in the specified comma-delimited list.

            query **adGroupIdFilter**:*string* | Optional. The returned array is filtered to include only ad groups with an identifier specified in the comma-delimited list.

            query **name**:*string* | Optional. The returned array includes only ad groups with the specified name.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/v2/sp/adGroups/extended/{}', method='GET')
    @Utils.deprecated
    def get_ad_group_extended(self, adGroupId, **kwargs) -> ApiResponse:
        r"""

        get_ad_group_extended(self, adGroupId, \*\*kwargs) -> ApiResponse

        Gets an ad group that has extended data fields.

            path **adGroupId**:*number* | Required. The identifier of an existing ad group.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), adGroupId), params=kwargs)
