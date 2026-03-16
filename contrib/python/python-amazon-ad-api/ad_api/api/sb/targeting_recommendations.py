from ad_api.base import Client, sp_endpoint, ApiResponse


class TargetsRecommendations(Client):
    """
    Use the Amazon Advertising API for Sponsored Brands for campaign, ad group, keyword, negative keyword, drafts, Stores, landing pages, and Brands management operations. For more information about Sponsored Brands, see the Sponsored Brands Support Center. For onboarding information, see the account setup topic.
    """

    @sp_endpoint('/sb/recommendations/targets/product/list', method='POST')
    def list_products_targets(self, **kwargs) -> ApiResponse:
        r"""
        Gets a list of recommended categories for targeting.

        Recommendations are based on the ASINs that are passed in the request.

        Request Body
            | '**nextToken**': *string*, {'description': 'Operations that return paginated results include a pagination token in this field. To retrieve the next page of results, call the same operation and specify this token in the request. If the NextToken field is empty, there are no further results.'}
            | '**maxResults**': *integer*, {'description': 'Sets a limit on the number of results returned by an operation. minimum: 1, maximum: 100'}
            | '**filters**': *SBExpression*, {'filterType': '[ ASINS ]', 'values': 'A list of ASINs / An ASIN.'}

        Returns:
            ApiResponse
        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sb/recommendations/targets/category', method='POST')
    def list_category_targets(self, **kwargs) -> ApiResponse:
        r"""
        Gets a list of recommended categories for targeting.

        Recommendations are based on the ASINs that are passed in the request.

        Request Body
            | '**asins**': *string*, {'description': 'A list of ASINs.'}

        Returns:
            ApiResponse
        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sb/recommendations/targets/brand', method='POST')
    def list_brand_targets(self, **kwargs) -> ApiResponse:
        r"""
        Gets a list of brand suggestions.

        The Brand suggestions are based on a list of either category identifiers or keywords passed in the request. It is not valid to specify both category identifiers and keywords in the request.

        Request Body (oneOf ->)
            | '**categoryId**': *integer($int64)*, {'description': 'The category identifier for which to get recommendations.'}
            | '**keyword**': *string*, {'description': 'The keyword for which to get recommendations.}

        Returns:
            ApiResponse
        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)
