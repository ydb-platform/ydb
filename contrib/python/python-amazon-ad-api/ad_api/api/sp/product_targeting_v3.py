from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class TargetsV3(Client):
    @sp_endpoint('/sp/targets/list', method='POST')
    def list_product_targets(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        Listing product targets.

        Request Body (optional)

        Returns
            ApiResponse
        """
        json_version = 'application/vnd.spTargetingClause.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/targets', method='POST')
    def create_product_targets(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Creating product targets.

        Request Body (required)
            | '**campaignId**': *string*, {'description': 'The number or recommendations returned in a single page.'}
            | '**adGroupId**': *string*, {'description': 'The page number in the result set to return.'}
            | '**expression**'
                | '**value**': *string*, {'description': 'The expression value.'}
                |  '**type**': *string*, {'description': '[ queryBroadMatches, queryPhraseMatches, queryExactMatches, asinCategorySameAs, asinBrandSameAs, asinPriceLessThan, asinPriceBetween, asinPriceGreaterThan, asinReviewRatingLessThan, asinReviewRatingBetween, asinReviewRatingGreaterThan, asinSameAs, queryBroadRelMatches, queryHighRelMatches, asinSubstituteRelated, asinAccessoryRelated, asinAgeRangeSameAs, asinGenreSameAs, asinIsPrimeShippingEligible ]'}
            | '**state**': *string*, {'description': 'The current resource state.' , 'Enum': '[ enabled, paused, archived ]'}
            | '**expressionType**': *string*, {'description': '[ auto, manual ]'}
            | '**bid**': *float*, {'description': 'The bid for ads sourced using the target. Min / Max 0.02 / 1000'}


        Returns
            ApiResponse
        """
        json_version = 'application/vnd.spTargetingClause.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/targets', method='PUT')
    def edit_product_targets(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Updating product targets.

        Request Body (required)
            | '**targetId**':  *string*, (required) {'description': 'The identifer of the campaign to which the keyword is associated.'}
            | '**state**': *string*, {'description': 'The current resource state.' , 'Enum': '[ enabled, paused, archived ]'}
            | '**bid**': *float* {'description': 'Bid associated with this keyword. Applicable to biddable match types only.'}
            | '**expression**'
            |       '**value**': *string*, The expression value.
            |       '**type**': *string*, The type of nagative targeting expression. You can only specify values for the following predicates: Enum : [ASIN_BRAND_SAME_AS, ASIN_SAME_AS]
            | '**expressionType**' Enum : [AUTO, MANUAL]

        Returns
            ApiResponse
        """
        json_version = 'application/vnd.spTargetingClause.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/targets/delete', method='POST')
    def delete_product_targets(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        Deleting product targets

        Request Body (required)
            | **targetIdFilter** {} : Filter product targets by the list of objectIds
                include [string] : list of productTargetIds as String to be used as filter. MinItems : 0, MaxItems :1000


        Returns
            ApiResponse
        """
        json_version = 'application/vnd.spTargetingClause.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    # New adding from Targets v2

    @sp_endpoint('/sp/targets/categories/recommendations', method='POST')
    def list_products_targets_categories_recommendations(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""

        Returns a list of category recommendations for the input list of ASINs. Use this API to discover relevant categories to target. To find ASINs, either use the Product Metadata API or browse the Amazon Retail Website.

        | header **Prefer**:*string* | Used to indicate the behavior preferred by the client but is not required for successful completion of the request. Supported values will be updated in the future.

        body: | REQUIRED {'description': 'An array of asins objects.}'

            | '**asins**': list>*string*, {'description': 'List of input ASINs. This API does not check if the ASINs are valid ASINs. maxItems: 10000.'}
            | '**includeAncestor**': *boolean*, {'description': 'Enable this if you would like to retrieve categories which are ancestor nodes of the original recommended categories. This may increase the number of categories returned, but decrease the relevancy of those categories.'}

        Returns:

            ApiResponse


        """

        schema_version = 'application/vnd.spproducttargeting.v' + str(version) + '+json'
        headers = {"Accept": schema_version, "Content-Type": schema_version}
        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})
        # contentType = 'application/vnd.spproducttargeting.v3+json'
        # headers = {'Content-Type': contentType}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    # TODO add the prefer header boolen and param the version in case upper version amazon release
    @sp_endpoint('/sp/targets/products/count', method='POST')
    def get_products_targets_count(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Get number of targetable asins based on refinements provided by the user. Please use the GetTargetableCategories API or the GetCategoryRecommendationsForASINs API to retrieve the category ID. Please use the GetRefinementsByCategory API to retrieve refinements data for a category.

            | header **Prefer**:*string* | Used to indicate the behavior preferred by the client but is not required for successful completion of the request. Supported values will be updated in the future.

        Returns:

            ApiResponse


        """

        schema_version = 'application/vnd.spproducttargeting.v' + str(version) + '+json'
        headers = {"Accept": schema_version, "Content-Type": schema_version}
        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    # TODO add the prefer header boolen
    @sp_endpoint('/sp/targets/categories', method='GET')
    def list_targets_categories(self, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Returns all targetable categories. This API returns a large JSON string containing a tree of category nodes. Each category node has the fields - category id, category name, and child categories.

            | header **Prefer**:*string* | Used to indicate the behavior preferred by the client but is not required for successful completion of the request. Supported values will be updated in the future.

        Returns:

            ApiResponse


        """
        headers = {}
        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})
        return self._request(kwargs.pop('path'), params=kwargs, headers=headers or False)

    @sp_endpoint('/sp/targets/category/{}/refinements', method='GET')
    def list_products_targets_category_refinements(self, categoryId, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Get a targeting clause specified by identifier.

            | path **categoryId**:*string* | Required. The target identifier.
            | header **Prefer**:*string* | Used to indicate the behavior preferred by the client but is not required for successful completion of the request. Supported values will be updated in the future.


        Returns:

            ApiResponse

        """

        headers = {}
        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})

        return self._request(fill_query_params(kwargs.pop('path'), categoryId), params=kwargs, headers=headers or False)
