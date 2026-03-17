from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class KeywordsV3(Client):
    @sp_endpoint('/sp/keywords/list', method='POST')
    def list_keywords(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        Listing product keywords.

        Request Body (optional)

        Returns
            ApiResponse
        """
        json_version = 'application/vnd.spKeyword.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/keywords', method='POST')
    def create_keyword(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Creating product keywords.

        Request Body (required)
            | **nativeLanguageKeyword** : (*string*), The unlocalized keyword text in the preferred locale of the advertiser
            | **nativeLanguageLocale** : (*string*), The locale preference of the advertiser.
            | **campaignId**: *string*, The identifer of the campaign to which the keyword is associated.
            | **adGroupId**: *string*, The identifier of the ad group to which this keyword is associated
            | **state**: *string*, The current resource state.' , 'Enum': '[ enabled ]
            | **keywordText**: *string*, The text of the expression to match against a search query.
            | **matchType**: *string*, 'The type of match.' , 'Enum': '[EXACT, PHRASE, BROAD]

        Returns
            ApiResponse
        """
        json_version = 'application/vnd.spKeyword.v' + str(version) + "+json"

        headers = {"Accept": json_version, "Content-Type": json_version}

        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/keywords', method='PUT')
    def edit_keyword(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Updating product keywords.

        Request Body (required)
            | '**keywordId**':  *string*, (required) {'description': 'The identifer of the campaign to which the keyword is associated.'}
            | '**state**': *string*, {'description': 'The current resource state.' , 'Enum': '[ enabled, paused, archived ]'}
            | '**bid**': *float* {'description': 'Bid associated with this keyword. Applicable to biddable match types only.'}

        Returns
            ApiResponse
        """

        json_version = 'application/vnd.spKeyword.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/keywords/delete', method='POST')
    def delete_keywords(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        Deleting product keywords.

        Request Body (required)
            | **keywordIdFilter** {} : Filter keywords by the list of objectIds
                include [string] : list of keywordsIds as String to be used as filter. MinItems : 0, MaxItems :1000

        Returns
            ApiResponse
        """

        json_version = 'application/vnd.spKeyword.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
