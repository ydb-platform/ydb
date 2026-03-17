from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class Targets(Client):
    """Amazon Advertising API - Sponsored Products - Product Targetting

    Use the Amazon Advertising API for Sponsored Products for campaign, ad group, keyword, negative keyword, and product
    ad management operations. For more information about Sponsored Products, see the Sponsored Products Support Center.
    For onboarding information, see the account setup topic.

    Version 2.0

    Doc: https://advertising.amazon.com/API/docs/en-us/sponsored-products/2-0/openapi#/Product%20targeting
    This specification is available for download from the `Advertising API Sponsored Products 2.0
    <https://d3a0d0y2hgofx6.cloudfront.net/openapi/en-us/sponsored-products/2-0/openapi.yaml>`_.

    Version 3.0

    Doc: https://advertising.amazon.com/API/docs/en-us/sponsored-products/3-0/openapi/prod#/Product%20Targeting
    This specification is available for download from the `Advertising API Sponsored Products 3.0
    <https://dtrnk0o2zy01c.cloudfront.net/openapi/en-us/dest/SponsoredProducts_prod_3p.json>`_.


    """

    @sp_endpoint('/v2/sp/targets', method='POST')
    @Utils.deprecated
    def create_products_targets(self, **kwargs) -> ApiResponse:
        r"""
        Creates one or more targeting expressions.

        body: | REQUIRED {'description': 'An array of asins objects.}'

            | '**campaignId**': *number*, {'description': 'The number or recommendations returned in a single page.'}
            | '**adGroupId**': *number*, {'description': 'The page number in the result set to return.'}
            | '**expression**'
            |       '**value**': *string*, {'description': 'The expression value.'}
            |       '**type**': *string*, {'description': '[ queryBroadMatches, queryPhraseMatches, queryExactMatches, asinCategorySameAs, asinBrandSameAs, asinPriceLessThan, asinPriceBetween, asinPriceGreaterThan, asinReviewRatingLessThan, asinReviewRatingBetween, asinReviewRatingGreaterThan, asinSameAs, queryBroadRelMatches, queryHighRelMatches, asinSubstituteRelated, asinAccessoryRelated, asinAgeRangeSameAs, asinGenreSameAs, asinIsPrimeShippingEligible ]'}
            | '**resolvedExpression**'
            |       '**value**': *string*, {'description': 'The expression value.'}
            |       '**type**': *string*, {'description': '[ queryBroadMatches, queryPhraseMatches, queryExactMatches, asinCategorySameAs, asinBrandSameAs, asinPriceLessThan, asinPriceBetween, asinPriceGreaterThan, asinReviewRatingLessThan, asinReviewRatingBetween, asinReviewRatingGreaterThan, asinSameAs, queryBroadRelMatches, queryHighRelMatches, asinSubstituteRelated, asinAccessoryRelated, asinAgeRangeSameAs, asinGenreSameAs, asinIsPrimeShippingEligible ]'}
            | '**expressionType**': *string*, {'description': '[ auto, manual ]'}
            | '**bid**': *number*, {'description': 'The bid for ads sourced using the target. Min / Max 0.02 / 1000'}

        Returns:

            ApiResponse


        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/v2/sp/targets', method='PUT')
    @Utils.deprecated
    def edit_products_targets(self, **kwargs) -> ApiResponse:
        r"""
        Updates one or more targeting clauses.

        body: | REQUIRED {'description': 'An array of asins objects.}'

            | '**targetId**': *number*, {'description': 'The target id.'}
            | '**state**': *string*, {'description': '[ enabled, paused, archived ]'}
            | '**expression**'
            |       '**value**': *string*, {'description': 'The expression value.'}
            |       '**type**': *string*, {'description': '[ queryBroadMatches, queryPhraseMatches, queryExactMatches, asinCategorySameAs, asinBrandSameAs, asinPriceLessThan, asinPriceBetween, asinPriceGreaterThan, asinReviewRatingLessThan, asinReviewRatingBetween, asinReviewRatingGreaterThan, asinSameAs, queryBroadRelMatches, queryHighRelMatches, asinSubstituteRelated, asinAccessoryRelated, asinAgeRangeSameAs, asinGenreSameAs, asinIsPrimeShippingEligible ]'}
            | '**resolvedExpression**'
            |       '**value**': *string*, {'description': 'The expression value.'}
            |       '**type**': *string*, {'description': '[ queryBroadMatches, queryPhraseMatches, queryExactMatches, asinCategorySameAs, asinBrandSameAs, asinPriceLessThan, asinPriceBetween, asinPriceGreaterThan, asinReviewRatingLessThan, asinReviewRatingBetween, asinReviewRatingGreaterThan, asinSameAs, queryBroadRelMatches, queryHighRelMatches, asinSubstituteRelated, asinAccessoryRelated, asinAgeRangeSameAs, asinGenreSameAs, asinIsPrimeShippingEligible ]'}
            | '**expressionType**': *string*, {'description': '[ auto, manual ]'}
            | '**bid**': *number*, {'description': 'The bid for ads sourced using the target. Min / Max 0.02 / 1000'}

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/v2/sp/targets', method='GET')
    @Utils.deprecated
    def list_products_targets(self, **kwargs) -> ApiResponse:
        r"""
        Gets a list of targeting clauses filtered by specified criteria.

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.

            query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.

            query **adGroupIdFilter**:*string* | Optional. Restricts results to keywords associated with ad groups specified by identifier in the comma-delimited list.

            query **targetIdFilter**:*string* | Optional. A comma-delimited list of target identifiers.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/v2/sp/targets/{}', method='GET')
    @Utils.deprecated
    def get_products_target(self, targetId, **kwargs) -> ApiResponse:
        r"""
        Get a targeting clause specified by identifier.

            path **targetId**:*number* | Required. The target identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), targetId), params=kwargs)

    @sp_endpoint('/v2/sp/targets/{}', method='DELETE')
    @Utils.deprecated
    def delete_products_target(self, targetId, **kwargs) -> ApiResponse:
        r"""
        Archives a targeting clause.

            path **targetId**:*number* | Required. The target identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), targetId), params=kwargs)

    @sp_endpoint('/v2/sp/targets/extended', method='GET')
    @Utils.deprecated
    def list_products_targets_extended(self, **kwargs) -> ApiResponse:
        r"""
        Gets a list of targeting clauses filtered by specified criteria.

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.

            query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.

            query **adGroupIdFilter**:*string* | Optional. Restricts results to keywords associated with ad groups specified by identifier in the comma-delimited list.

            query **targetIdFilter**:*string* | Optional. A comma-delimited list of target identifiers.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/v2/sp/targets/extended/{}', method='GET')
    @Utils.deprecated
    def get_products_target_extended(self, targetId, **kwargs) -> ApiResponse:
        r"""
        Get a targeting clause specified by identifier.

            path **targetId**:*number* | Required. The target identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), targetId), params=kwargs)

    @sp_endpoint('/v2/sp/targets/productRecommendations', method='POST')
    @Utils.deprecated
    def get_products_targets_recommendations(self, **kwargs) -> ApiResponse:
        r"""

        Gets a list of recommended products for targeting.

        body: | REQUIRED {'description': 'An array of asins objects.}'

            | '**pageSize**': *number*, {'description': 'The number or recommendations returned in a single page.'}
            | '**pageNumber**': *number*, {'description': 'The page number in the result set to return.'}
            | '**asins**': list>*string*, {'description': 'A list of ASINs.'}

        Returns:

            ApiResponse


        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/v2/sp/targets/brands', method='GET')
    @Utils.deprecated
    def get_brand_targets(self, **kwargs) -> ApiResponse:
        r"""
        Gets a list of recommended products for targeting.

            path **keyword**:*string* | Optional Unique exclude categoryId. A keyword for which to get recommended brands.
            path **categoryId**:*number* | Optional Unique exclude keyword. Gets the top 50 brands for the specified category identifier.


        Returns:

            ApiResponse


        """
        return self._request(kwargs.pop('path'), params=kwargs)

    """
    Sponsored Products Targeting API. v3.0
    """

    @sp_endpoint('/sp/targets/categories/recommendations', method='POST')
    def list_products_targets_categories_recommendations(self, **kwargs) -> ApiResponse:
        r"""

        Returns a list of category recommendations for the input list of ASINs. Use this API to discover relevant categories to target. To find ASINs, either use the Product Metadata API or browse the Amazon Retail Website.

        | header **Prefer**:*string* | Used to indicate the behavior preferred by the client but is not required for successful completion of the request. Supported values will be updated in the future.

        body: | REQUIRED {'description': 'An array of asins objects.}'

            | '**asins**': list>*string*, {'description': 'List of input ASINs. This API does not check if the ASINs are valid ASINs. maxItems: 10000.'}
            | '**includeAncestor**': *boolean*, {'description': 'Enable this if you would like to retrieve categories which are ancestor nodes of the original recommended categories. This may increase the number of categories returned, but decrease the relevancy of those categories.'}

        Returns:

            ApiResponse


        """
        contentType = 'application/vnd.spproducttargeting.v3+json'
        headers = {'Content-Type': contentType}
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs, headers=headers)

    @sp_endpoint('/sp/negativeTargets/brands/recommendations', method='GET')
    def list_negative_targets_brands_recommendations(self, **kwargs) -> ApiResponse:
        r"""
        Returns brands recommended for negative targeting. Only available for Sellers and Vendors. These recommendations include your own brands because targeting your own brands usually results in lower performance than targeting competitors' brands.

            | header **Prefer**:*string* | Used to indicate the behavior preferred by the client but is not required for successful completion of the request. Supported values will be updated in the future.

        Returns:

            ApiResponse


        """
        contentType = 'application/vnd.spproducttargeting.v3+json'
        headers = {'Content-Type': contentType}
        return self._request(kwargs.pop('path'), params=kwargs, headers=headers)

    @sp_endpoint('/sp/targets/products/count', method='POST')
    def get_products_targets_count(self, **kwargs) -> ApiResponse:
        r"""
        Get number of targetable asins based on refinements provided by the user. Please use the GetTargetableCategories API or the GetCategoryRecommendationsForASINs API to retrieve the category ID. Please use the GetRefinementsByCategory API to retrieve refinements data for a category.

            | header **Prefer**:*string* | Used to indicate the behavior preferred by the client but is not required for successful completion of the request. Supported values will be updated in the future.

        Returns:

            ApiResponse


        """

        contentType = 'application/vnd.spproducttargeting.v3+json'
        headers = {'Content-Type': contentType}
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs, headers=headers)

    @sp_endpoint('/sp/targets/categories', method='GET')
    def list_targets_categories(self, **kwargs) -> ApiResponse:
        r"""
        Returns all targetable categories. This API returns a large JSON string containing a tree of category nodes. Each category node has the fields - category id, category name, and child categories.

            | header **Prefer**:*string* | Used to indicate the behavior preferred by the client but is not required for successful completion of the request. Supported values will be updated in the future.

        Returns:

            ApiResponse


        """
        return self._request(kwargs.pop('path'), params=kwargs, headers=False)

    @sp_endpoint('/sp/negativeTargets/brands/search', method='POST')
    def list_negative_targets_brands_search(self, **kwargs) -> ApiResponse:
        r"""
        Returns brands related to keyword input for negative targeting.

            | header **Prefer**:*string* | Used to indicate the behavior preferred by the client but is not required for successful completion of the request. Supported values will be updated in the future.

        Returns:

            ApiResponse


        """

        contentType = 'application/vnd.spproducttargeting.v3+json'
        headers = {'Content-Type': contentType}
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs, headers=headers)

    @sp_endpoint('/sp/targets/category/{}/refinements', method='GET')
    def list_products_targets_category_refinements(self, categoryId, **kwargs) -> ApiResponse:
        r"""
        Get a targeting clause specified by identifier.

            | path **categoryId**:*string* | Required. The target identifier.
            | header **Prefer**:*string* | Used to indicate the behavior preferred by the client but is not required for successful completion of the request. Supported values will be updated in the future.


        Returns:

            ApiResponse

        """

        return self._request(fill_query_params(kwargs.pop('path'), categoryId), params=kwargs, headers=False)
