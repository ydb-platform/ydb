from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class CampaignNegativeKeywords(Client):
    @sp_endpoint('/v2/sp/campaignNegativeKeywords/{}', method='GET')
    @Utils.deprecated
    def get_campaign_negative_keyword(self, keywordId, **kwargs) -> ApiResponse:
        r"""

        get_campaign_negative_keyword(self, keywordId, \*\*kwargs) -> ApiResponse

        Gets a campaign negative keyword specified by identifier.

            path **keywordId**:*number* | Required. The identifier of an existing keyword.


        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), keywordId), params=kwargs)

    @sp_endpoint('/v2/sp/campaignNegativeKeywords/{}', method='DELETE')
    @Utils.deprecated
    def delete_campaign_negative_keyword(self, keywordId, **kwargs) -> ApiResponse:
        r"""

        delete_campaign_negative_keyword(self, keywordId, \*\*kwargs) -> ApiResponse

        Archives a campaign negative keyword.

            path **keywordId**:*number* | Required. The identifier of an existing keyword.


        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), keywordId), params=kwargs)

    @sp_endpoint('/v2/sp/campaignNegativeKeywords/extended/{}', method='GET')
    @Utils.deprecated
    def get_campaign_negative_keyword_extended(self, keywordId, **kwargs) -> ApiResponse:
        r"""

        get_campaign_negative_keyword_extended(self, keywordId, \*\*kwargs) -> ApiResponse

        Gets a campaign negative keyword that has extended data fields.

            path **keywordId**:*number* | Required. The identifier of an existing keyword.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), keywordId), params=kwargs)

    @sp_endpoint('/v2/sp/campaignNegativeKeywords/extended', method='GET')
    @Utils.deprecated
    def list_campaign_negative_keywords_extended(self, **kwargs) -> ApiResponse:
        r"""
        list_campaign_negative_keywords_extended(self, \*\*kwargs) -> ApiResponse

        Gets a list of campaign negative keywords that have extended data fields.

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **matchTypeFilter**:*string* | Optional. Restricts results to keywords with match types within the specified comma-separated list. Available values : negativePhrase, negativeExact.

            query **keywordText**:*string* | Optional. Restricts results to keywords that match the specified text exactly.

            query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.

            query **keywordIdFilter**:*string* | Optional. Restricts results to keywords associated with campaigns specified by identifier in the comma-delimited list.

            Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/v2/sp/campaignNegativeKeywords', method='GET')
    @Utils.deprecated
    def list_campaign_negative_keywords(self, **kwargs) -> ApiResponse:
        r"""
        list_campaign_negative_keywords(self, \*\*kwargs) -> ApiResponse

        Gets a list of campaign negative keywords.

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **matchTypeFilter**:*string* | Optional. Restricts results to keywords with match types within the specified comma-separated list. Available values : negativePhrase, negativeExact.

            query **keywordText**:*string* | Optional. Restricts results to keywords that match the specified text exactly.

            query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.

            query **keywordIdFilter**:*string* | Optional. Restricts results to keywords associated with campaigns specified by identifier in the comma-delimited list.

            Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/v2/sp/campaignNegativeKeywords', method='POST')
    @Utils.deprecated
    def create_campaign_negative_keywords(self, **kwargs) -> ApiResponse:
        r"""
        create_campaign_negative_keywords(self, \*\*kwargs) -> ApiResponse:

        Creates one or more campaign negative keywords.

        body: | REQUIRED {'description': 'An array of keyword objects.}'

            | '**campaignId**': *number*, {'description': 'The identifer of the campaign to which the keyword is associated.'}
            | '**state**': *string*, {'description': 'The current resource state.' , 'Enum': '[ enabled ]'}
            | '**keywordText**': *string*, {'description': 'The text of the expression to match against a search query.'}
            | '**matchType**': *string*, {'description': 'The type of match.' , 'Enum': '[ negativeExact, negativePhrase ]'}

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/v2/sp/campaignNegativeKeywords', method='PUT')
    @Utils.deprecated
    def edit_campaign_negative_keywords(self, **kwargs) -> ApiResponse:
        r"""
        edit_campaign_negative_keywords(self, \*\*kwargs) -> ApiResponse:

        Updates one or more campaign negative keywords.

        body: | REQUIRED {'description': 'An array of campaign negative keywords with updated values.'}

            | '**keywordId**': *number*, {'description': 'The identifer of the campaign to which the keyword is associated.'}
            | '**state**': *string*, {'description': 'The current resource state.' , 'Enum': '[ deleted ]'}

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)
