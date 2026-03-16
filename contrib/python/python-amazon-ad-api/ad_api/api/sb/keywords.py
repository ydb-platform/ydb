from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class Keywords(Client):
    """
    Use the Amazon Advertising API for Sponsored Brands for campaign, ad group, keyword, negative keyword, drafts, Stores, landing pages, and Brands management operations. For more information about Sponsored Brands, see the Sponsored Brands Support Center. For onboarding information, see the account setup topic.
    """

    @sp_endpoint('/sb/keywords', method='GET')
    def list_keywords(self, version: float = 3.2, **kwargs) -> ApiResponse:
        r"""
        Gets an array of keywords, filtered by optional criteria.

        Keyword Args
            | query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0
            | query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.
            | query **matchTypeFilter**:*string* | Optional. Restricts results to keywords with match types within the specified comma-separated list.. Available values : broad, phrase, exact.
            | query **keywordText**:*string* | Optional. Restricts results to keywords that match the specified text exactly.
            | query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.
            | query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.
            | query **adGroupIdFilter**:*string* | Optional. Restricts results to keywords associated with ad groups specified by identifier in the comma-delimited list.
            | query **keywordIdFilter**:*string* | Optional. Restricts results to keywords associated with campaigns specified by identifier in the comma-delimited list..
            | query **creativeType**:*string* | Optional. Filter by the type of creative the campaign is associated with. To get ad groups associated with non-video campaigns specify 'productCollection'. To get ad groups associated with video campaigns, this must be set to 'video'. Returns all ad groups if not specified. Available values : productCollection, video
            | query **locale**:*string* | Optional. Restricts results to keywords associated with locale.

        Returns:
            | ApiResponse

        """
        json_version = 'application/vnd.sbkeyword.v' + str(version) + "+json"
        headers = {'Accept': json_version}
        return self._request(kwargs.pop('path'), params=kwargs, headers=headers)

    @sp_endpoint('/sb/keywords', method='PUT')
    def edit_keywords(self, **kwargs) -> ApiResponse:
        """
        Updates one or more keywords.

        Keywords submitted for update may have state set to pending for moderation review. Moderation may take up to 72 hours.
        Note that keywords can be updated on campaigns where serving status is not one of archived, terminated, rejected, or ended.
        Note that this operation supports a maximum list size of 100 keywords.

        Request body
            | **keywordId** (integer($int64)): [required] The identifier of the keyword.
            | **adGroupId** (integer($int64)): [required] The identifier of an existing ad group to which the keyword is associated.
            | **campaignId** (integer($int64)) [required] The identifier of an existing campaign to which the keyword is associated.
            | **state** (string): [optional] Newly created SB keywords are in a default state of 'draft' before transitioning to a 'pending' state for moderation. After moderation, the keyword will be in an enabled state. Enum: [ enabled, paused, pending, archived, draft ]
            | **bid** (number) [optional] The bid associated with the keyword. Note that this value must be less than the budget associated with the Advertiser account. For more information, see the Keyword bid constraints by marketplace section of the supported features article

        Returns:
            | ApiResponse
        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sb/keywords', method='POST')
    def create_keywords(self, **kwargs) -> ApiResponse:
        """
        Creates one or more keywords.

        Note that state can't be set at keyword creation. Keywords submitted for creation have state set to pending while under moderation review. Moderation review may take up to 72 hours.

        Note that keywords can be created on campaigns where serving status is not one of archived, terminated, rejected, or ended.

        Note that this operation supports a maximum list size of 100 keywords.

        Request body
            | **adGroupId** (integer($int64)) The identifier of an existing ad group to which the keyword is associated.
            | **campaignId** (integer($int64)) The identifier of an existing campaign to which the keyword is associated.
            | **keywordText** (string) The keyword text. The maximum number of words for this string is 10.
            | **nativeLanguageKeyword** (string) The unlocalized keyword text in the preferred locale of the advertiser.
            | **nativeLanguageLocale** (string) The locale preference of the advertiser. For example, if the advertiser’s preferred language is Simplified Chinese, set the locale to zh_CN. Supported locales include: Simplified Chinese (locale: zh_CN) for US, UK and CA. English (locale: en_GB) for DE, FR, IT and ES.
            | **matchType** (string) The match type. For more information, see match types in the Amazon Advertising support center. Enum: [ broad, exact, phrase ]
            | **bid** (number) [optional] The bid associated with the keyword. Note that this value must be less than the budget associated with the Advertiser account. For more information, see the Keyword bid constraints by marketplace section of the supported features article.

        Returns:
            | ApiResponse
        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sb/keywords/{}', method='GET')
    def get_keyword(self, keywordId, **kwargs) -> ApiResponse:
        """
        Gets a keyword specified by identifier.

        Keyword Args
            | path **keywordId** (integer): The identifier of an existing keyword. [required]
            | query **locale** (string): The returned array includes only keywords associated with locale matching those specified by identifier. [optional]

        Returns:
            | ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), keywordId), params=kwargs)

    @sp_endpoint('/sb/keywords/{}', method='DELETE')
    def delete_keyword(self, keywordId, **kwargs) -> ApiResponse:
        """
        Archives a keyword specified by identifier.

        This operation is equivalent to an update operation that sets the status field to 'archived'. Note that setting the status field to 'archived' is permanent and can't be undone. See Developer Notes for more information.

        Keyword Args
            | path **keywordId** (integer): The identifier of an existing keyword. [required]


        Returns:
            | ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), keywordId), params=kwargs)
