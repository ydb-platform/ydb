from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class NegativeKeywords(Client):
    """
    Use the Amazon Advertising API for Sponsored Brands for campaign, ad group, keyword, negative keyword, drafts, Stores, landing pages, and Brands management operations. For more information about Sponsored Brands, see the Sponsored Brands Support Center. For onboarding information, see the account setup topic.
    """

    @sp_endpoint('/sb/negativeKeywords', method='GET')
    def list_negative_keywords(self, **kwargs) -> ApiResponse:
        r"""

        Gets an array of negative keywords, filtered by optional criteria.

        Keyword Args
            | query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0
            | query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.
            | query **matchTypeFilter**:*string* | Optional. Restricts results to keywords with match types within the specified comma-separated list. Available values : negativePhrase, negativeExact.
            | query **keywordText**:*string* | Optional. Restricts results to keywords that match the specified text exactly.
            | query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values :  enabled, archived.
            | query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.
            | query **adGroupIdFilter**:*string* | Optional. Restricts results to keywords associated with ad groups specified by identifier in the comma-delimited list.
            | query **keywordIdFilter**:*string* | Optional. Restricts results to keywords associated with campaigns specified by identifier in the comma-delimited list.
            | query **creativeType**:*string* | Optional. Filter by the type of creative the campaign is associated with. To get negative keywords associated with non-video campaigns specify 'productCollection'. To get negative keywords associated with video campaigns, this must be set to 'video'. Returns all negative keywords if not specified. Available values : productCollection, video
        Returns:
            | ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sb/negativeKeywords', method='PUT')
    def edit_negative_keywords(self, **kwargs) -> ApiResponse:
        r"""

        Updates one or more negative keywords.

        Negative keywords submitted for update may have state set to pending for moderation review. Moderation may take up to 72 hours.
        Note that negative keywords can be updated on campaigns where serving status is not one of archived, terminated, rejected, or ended.
        Note that this operation supports a maximum list size of 100 negative keywords.

        Request Body
            | '**keywordId**': *number*, {'description': 'The identifier of the negative keyword.'}
            | '**adGroupId**': *number*, {'description': 'The identifier of the ad group to which the negative keyword is associated.'}
            | '**campaignId**': *number*, {'description': 'The identifier of the campaign to which the negative keyword is associated.'}
            | '**state**': *string*, {'description': 'The current state of the negative keyword. Newly created SB negative keywords are in a default state of 'draft' before transitioning to a 'pending' state for moderation review. 'enabled' refers to negative keywords that are active. 'archived' refers to negative keywords that are permanently inactive and cannot be returned to the 'enabled' state.' , 'Enum': 'enabled, pending, archived, draft'}

        Returns:
            ApiResponse

        """
        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs)

    @sp_endpoint('/sb/negativeKeywords', method='POST')
    def create_negative_keywords(self, **kwargs) -> ApiResponse:
        r"""
        Creates one or more negative keywords.

        Note that bid and state can't be set at negative keyword creation.

        Note that Negative keywords submitted for creation have state set to pending while under moderation review. Moderation review may take up to 72 hours.

        Note that negative keywords can be created on campaigns one where serving status is not one of archived, terminated, rejected, or ended.

        Note that this operation supports a maximum list size of 100 negative keywords.

        Request Body
            | '**campaignId**': *number*, {'description': 'The identifer of the campaign to which the keyword is associated.'}
            | '**adGroupId**': *number*, {'description': 'The identifier of the ad group to which this keyword is associated.'}
            | '**state**': *string*, {'description': 'The current resource state.' , 'Enum': '[ enabled ]'}
            | '**keywordText**': *string*, {'description': 'The text of the expression to match against a search query.'}
            | '**matchType**': *string*, {'description': 'The type of match.' , 'Enum': '[ negativeExact, negativePhrase ]'}

        Returns:
            ApiResponse

        """
        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs)

    @sp_endpoint('/sb/negativeKeywords/{}', method='GET')
    def get_negative_keyword(self, keywordId, **kwargs) -> ApiResponse:
        r"""

        Gets a negative keyword specified by identifier.

        Keyword Args
            path **keywordId**:*number* | Required. The identifier of an existing keyword.


        Returns
            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), keywordId), params=kwargs)

    @sp_endpoint('/sb/negativeKeywords/{}', method='DELETE')
    def delete_negative_keyword(self, keywordId, **kwargs) -> ApiResponse:
        r"""

        Archives a negative keyword specified by identifier.

        This operation is equivalent to an update operation that sets the status field to 'archived'. Note that setting the status field to 'archived' is permanent and can't be undone. See Developer Notes for more information.

        Keyword Args
            path **keywordId**:*number* | Required. The identifier of an existing keyword.

        Returns
            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), keywordId), params=kwargs)
