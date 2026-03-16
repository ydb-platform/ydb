from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class Keywords(Client):
    @sp_endpoint('/v2/sp/keywords/{}', method='GET')
    @Utils.deprecated
    def get_keyword(self, keywordId, **kwargs) -> ApiResponse:
        r"""

        get_keyword(self, keywordId, **kwargs) -> ApiResponse

        Gets a keyword specified by identifier.

            path **keywordId**:*number* | Required. The identifier of an existing keyword.
            query **locale**:*number* | Optional. The locale preference of the advertiser.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), keywordId), params=kwargs)

    @sp_endpoint('/v2/sp/keywords/{}', method='DELETE')
    @Utils.deprecated
    def delete_keyword(self, keywordId, **kwargs) -> ApiResponse:
        r"""
        delete_keyword(self, keywordId, **kwargs) -> ApiResponse:

        Archives a keyword.

            path **keywordId**:*number* | Required. The identifier of an existing keyword.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), keywordId), params=kwargs)

    @sp_endpoint('/v2/sp/keywords/extended/{}', method='GET')
    @Utils.deprecated
    def get_keyword_extended(self, keywordId, **kwargs) -> ApiResponse:
        r"""

        get_keyword_extended(self, keywordId, **kwargs) -> ApiResponse

        Gets a keyword with extended data fields.

            path **keywordId**:*number* | Required. The identifier of an existing keyword.
            query **locale**:*number* | Optional. The locale preference of the advertiser.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), keywordId), params=kwargs)

    @sp_endpoint('/v2/sp/keywords/extended', method='GET')
    @Utils.deprecated
    def list_keywords_extended(self, **kwargs) -> ApiResponse:
        r"""
        list_keywords_extended(self, **kwargs) -> ApiResponse

        Gets a list of keywords that have extended data fields.

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **matchTypeFilter**:*string* | Optional. Restricts results to keywords with match types within the specified comma-separated list.. Available values : broad, phrase, exact.

            query **keywordText**:*string* | Optional. Restricts results to keywords that match the specified text exactly.

            query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.

            query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.

            query **adGroupIdFilter**:*string* | Optional. Restricts results to keywords associated with ad groups specified by identifier in the comma-delimited list.

            query **keywordIdFilter**:*string* | Optional. Restricts results to keywords associated with campaigns specified by identifier in the comma-delimited list..

            query **locale**:*string* | Optional. Restricts results to keywords associated with locale.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/v2/sp/keywords', method='GET')
    @Utils.deprecated
    def list_keywords(self, **kwargs) -> ApiResponse:
        r"""
        list_keywords(self, **kwargs) -> ApiResponse

        Gets a list of keywords.

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **matchTypeFilter**:*string* | Optional. Restricts results to keywords with match types within the specified comma-separated list.. Available values : broad, phrase, exact.

            query **keywordText**:*string* | Optional. Restricts results to keywords that match the specified text exactly.

            query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.

            query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.

            query **adGroupIdFilter**:*string* | Optional. Restricts results to keywords associated with ad groups specified by identifier in the comma-delimited list.

            query **keywordIdFilter**:*string* | Optional. Restricts results to keywords associated with campaigns specified by identifier in the comma-delimited list..

            query **locale**:*string* | Optional. Restricts results to keywords associated with locale.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/v2/sp/keywords', method='POST')
    @Utils.deprecated
    def create_keywords(self, **kwargs) -> ApiResponse:
        r"""
        create_keywords(self, **kwargs) -> ApiResponse:

        Creates one or more keywords.

        body: | REQUIRED {'description': 'An array of keyword objects.}'

            | '**campaignId**': *number*, {'description': 'The identifer of the campaign to which the keyword is associated.'}
            | '**adGroupId**': *number*, {'description': 'The identifier of the ad group to which this keyword is associated.'}
            | '**state**': *string*, {'description': 'The current resource state.' , 'Enum': '[ enabled, paused, archived ]'}
            | '**keywordText**': *string*, {'description': 'The keyword text.'}
            | '**nativeLanguageKeyword**': *string*, {'description': 'The unlocalized keyword text in the preferred locale of the advertiser.'}
            | '**nativeLanguageLocale**': *string*, {'description': 'The locale preference of the advertiser.'}
            | '**matchType**': *string*, {'description': 'The type of match.' , 'Enum': '[ exact, phrase, broad ]'}
            | '**bid**': *number($float)* {'description': 'Bid associated with this keyword. Applicable to biddable match types only.'}

        Returns:

            ApiResponse


        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/v2/sp/keywords', method='PUT')
    @Utils.deprecated
    def edit_keywords(self, **kwargs) -> ApiResponse:
        r"""
        edit_keywords(self, **kwargs) -> ApiResponse:

        Updates one or more keywords.

        body: | REQUIRED {'description': 'An array of keyword objects.}'

            | '**keywordId**': *number*, {'description': 'The identifer of the campaign to which the keyword is associated.'}
            | '**state**': *string*, {'description': 'The current resource state.' , 'Enum': '[ enabled, paused, archived ]'}
            | '**bid**': *number($float)* {'description': 'Bid associated with this keyword. Applicable to biddable match types only.'}

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)
