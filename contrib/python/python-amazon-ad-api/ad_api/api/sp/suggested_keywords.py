from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class SuggestedKeywords(Client):
    @sp_endpoint('/v2/sp/adGroups/{}/suggested/keywords', method='GET')
    def get_keywords(self, adGroupId, **kwargs) -> ApiResponse:
        r"""
        get_keywords_request(self, adGroupId, \*\*kwargs) -> ApiResponse

        Gets suggested keywords for the specified ad group.

            path adGroupId:*number* | Required. The identifier of a valid ad group.

            query maxNumSuggestions:*integer* | Optional. The maxiumum number of suggested keywords for the response. Default value 100.

            query adStateFilter:*string* | Optional. Filters results to ad groups with state matching the comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived.


        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), adGroupId), params=kwargs)

    @sp_endpoint('/v2/sp/adGroups/{}/suggested/keywords/extended', method='GET')
    def get_keywords_extended(self, adGroupId, **kwargs) -> ApiResponse:
        r"""
        get_keywords_extended(self, adGroupId, \*\*kwargs) -> ApiResponse

        Gets suggested keywords with extended data for the specified ad group.

            path adGroupId:*number* | Required. The identifier of a valid ad group.

            query maxNumSuggestions:*integer* | Optional. The maxiumum number of suggested keywords for the response. Default value 100.

            query suggestBids:*string* | Optional. Set to yes to include a suggest bid for the suggested keyword in the response. Otherwise, set to no. Available values : yes, no. Default value : no.

            query adStateFilter:*string* | Optional. Filters results to ad groups with state matching the comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived.


        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), adGroupId), params=kwargs)

    @sp_endpoint('/v2/sp/asins/{}/suggested/keywords', method='GET')
    def get_asin_keywords(self, asinValue, **kwargs) -> ApiResponse:
        r"""
        get_asin_keywords(self, asinValue, \*\*kwargs) -> ApiResponse

        Gets suggested keywords for the specified ad group.

            path asinValue:*string* | Required. An ASIN.

            query maxNumSuggestions:*integer* | Optional. The maxiumum number of suggested keywords for the response. Default value 100.


        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), asinValue), params=kwargs)

    @sp_endpoint('/v2/sp/asins/suggested/keywords', method='POST')
    def get_asins_keywords(self, **kwargs) -> ApiResponse:
        r"""
        get_asins_keywords(self, \*\*kwargs) -> ApiResponse:

        Gets suggested keyword for a specified list of ASINs.

        body: | REQUIRED {'description': 'An object with ASINs.}'

            | 'asins': *string*, {'description': 'A list of ASINs.'}
            | 'maxNumSuggestions': *integer*, {'description': 'The maximum number of suggested keywords in the response. minItems: 1. maxItems: 1000. default: 100'}


        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)
