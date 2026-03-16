from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class AdGroups(Client):
    """
    Use the Amazon Advertising API for Sponsored Brands for campaign, ad group, keyword, negative keyword, drafts, Stores, landing pages, and Brands management operations. For more information about Sponsored Brands, see the Sponsored Brands Support Center. For onboarding information, see the account setup topic.
    """

    @sp_endpoint('/sb/adGroups', method='GET')
    @Utils.deprecated
    def list_ad_groups(self, **kwargs) -> ApiResponse:
        r"""

        Gets an array of ad groups associated with the client identifier passed in the authorization header, filtered by specified criteria.

        Keyword Args
            | query **startIndex**:*integer* | Optional. Sets a cursor into the requested set of campaigns. Use in conjunction with the count parameter to control pagination of the returned array. 0-indexed record offset for the result set, defaults to 0.

            | query **count**:*integer* | Optional. Sets the number of AdGroup objects in the returned array. Use in conjunction with the startIndex parameter to control pagination. For example, to return the first ten ad groups set startIndex=0 and count=10. To return the next ten ad groups, set startIndex=10 and count=10, and so on. Defaults to max page size.

            | query **name**:*string* | Optional. The returned array includes only ad groups with the specified name.

            | query **adGroupIdFilter**:*string* | Optional. The returned array is filtered to include only ad groups with an identifier specified in the comma-delimited list.

            | query **campaignIdFilter**:*string* | Optional. The returned array is filtered to include only ad groups associated with the campaign identifiers in the specified comma-delimited list.

            | query **creativeType**:*string* | Optional. Filter by the type of creative the campaign is associated with. To get ad groups associated with non-video campaigns specify 'productCollection'. To get ad groups associated with video campaigns, this must be set to 'video'. Returns all ad groups if not specified. Available values : productCollection, video

        Returns:
            | ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sb/adGroups/{}', method='GET')
    def get_ad_group(self, adGroupId, **kwargs) -> ApiResponse:
        r"""

        get_ad_group(self, adGroupId, \*\*kwargs) -> ApiResponse

        Gets an ad group specified by identifier.

        Keyword Args
            | path **adGroupId**:*number* | Required. The identifier of an existing ad group.

        Returns:
            | ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), adGroupId), params=kwargs)
