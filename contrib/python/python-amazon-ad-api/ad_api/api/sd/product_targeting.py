from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class Targets(Client):
    """Amazon Advertising API for Sponsored Display

    Documentation: https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Targeting

    This API enables programmatic access for campaign creation, management, and reporting for Sponsored Display campaigns. For more information on the functionality, see the `Sponsored Display Support Center <https://advertising.amazon.com/help#GTPPHE6RAWC2C4LZ>`_ . For API onboarding information, see the `account setup <https://advertising.amazon.com/API/docs/en-us/setting-up/account-setup>`_  topic.

    This specification is available for download from the `Advertising API developer portal <https://d3a0d0y2hgofx6.cloudfront.net/openapi/en-us/sponsored-display/3-0/openapi.yaml>`_.

    """

    @sp_endpoint('/sd/targets', method='GET')
    def list_products_targets(self, **kwargs) -> ApiResponse:
        r"""
        Gets a list of targeting clauses objects for a requested set of Sponsored Display targets. Note that the Targeting Clause object is designed for performance, and includes a small set of commonly used fields to reduce size. If the extended set of fields is required, use the target operations that return the TargetingClauseEx object.

            | query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Defaults to 0.
            | query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.
            | query **stateFilter**:*string* | Optional. Optional. Restricts results to those with state set to values in the specified comma-separated list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived. Default value : enabled, paused, archived.
            | query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.
            | query **adGroupIdFilter**:*string* | Optional. Optional list of comma separated adGroupIds. Restricts results to targeting clauses with the specified adGroupId
            | query **targetIdFilter**:*string* | Optional. A comma-delimited list of target identifiers. Missing in Amazon documentation.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sd/targets', method='POST')
    def create_products_targets(self, **kwargs) -> ApiResponse:
        r"""
        Creates one or more targeting expressions.

        body: | REQUIRED {'description': 'A list of up to 100 targeting clauses for creation.}'

            | '**state**': *string*, {'description': '[ enabled, paused, archived ]'}
            | '**bid**': *number($float)*, {'description': 'The bid will override the adGroup bid if specified. This field is not used for negative targeting clauses. The bid must be less than the maximum allowable bid for the campaign's marketplace; for a list of maximum allowable bids, find the "Bid constraints by marketplace" table in our documentation overview.'}
            | '**adGroupId**': *integer($int64)*, {'description': 'The page number in the result set to return.'}
            | '**expressionType**': *string*, {'description': 'Tactic T00020 ad groups only allow manual targeting. [ manual, auto ]'}
            | '**expression**': *CreateTargetingExpression*

            CreateTargetingExpression:
            type: array
            description: The targeting expression to match against.

            Applicable to Product targeting (T00020)

            * A 'TargetingExpression' in a Product targeting Campaign can only contain 'TargetingPredicate' components.
            * Expressions must specify either a category predicate or an ASIN predicate, but never both.
            * Only one category may be specified per targeting expression.
            * Only one brand may be specified per targeting expression.
            * Only one asin may be specified per targeting expression.
            * To exclude a brand from a targeting expression you must create a negative targeting expression in the same ad group as the positive targeting expression.

            Applicable to Audience targeting (T00030)

            * A 'TargetingExpression' in an Audience Campaign can only contain 'TargetingPredicateNested' components.
            * Expressions must specify either auto ASIN-grain (exact products), manual ASIN-grain (similar products), or manual category-grain targeting.
            * **Future** To exclude parts of an audience, specify a TargetingPredicateNested component that contains a negative TargetingPredicate type.

            oneOf->

            TargetingPredicate:

                type: object
                description: A predicate to match against in the Targeting Expression (only applicable to Product targeting - T00020).

                * All IDs passed for category and brand-targeting predicates must be valid IDs in the Amazon Advertising browse system.
                * Brand, price, and review predicates are optional and may only be specified if category is also specified.
                * Review predicates accept numbers between 0 and 5 and are inclusive.
                * When using either of the 'between' strings to construct a targeting expression the format of the string is 'double-double' where the first double must be smaller than the second double. Prices are not inclusive.

                | '**type**': *string*, {'enum': '[ asinSameAs, asinCategorySameAs, asinBrandSameAs, asinPriceBetween, asinPriceGreaterThan, asinPriceLessThan, asinReviewRatingLessThan, asinReviewRatingGreaterThan, asinReviewRatingBetween, asinIsPrimeShippingEligible, asinAgeRangeSameAs, asinGenreSameAs, similarProduct ]'}
                | '**value**': *string*, {'description': 'The value to be targeted. example: B0123456789'}

            TargetingPredicateNested:

                type: object
                description: A behavioral event and list of targeting predicates that represents an Audience to target (only applicable to Audience targeting - T00030).

                * For auto ASIN-grain targeting, the value array must contain only 'exactProduct' and 'lookback' TargetingPredicateBase components.
                * For manual ASIN-grain targeting, the value array must contain only 'similarProduct' and 'lookback' TargetingPredicateBase components.
                * For manual Category-grain targeting, the value array must contain a 'lookback' and 'asinCategorySameAs' TargetingPredicateBase component, which can be further refined with optional brand, price, star-rating and shipping eligibility refinements.
                * For Amazon Audiences targeting, the TargetingPredicateNested type should be set to 'audience' and the value array should include one TargetingPredicateBase component with type set to 'audienceSameAs'.
                * **Future** For manual Category-grain targeting, adding a 'negative' TargetingPredicateBase will exclude that TargetingPredicateNested from the overall audience.


                | '**type**': *string*, {'enum': '[ views, audience, purchases ]'}
                | '**value**': TargetingPredicateBase

                    type: object

                    description: A predicate to match against inside the TargetingPredicateNested component (only applicable to Audience targeting - T00030).

                    * All IDs passed for category and brand-targeting predicates must be valid IDs in the Amazon Advertising browse system.
                    * Brand, price, and review predicates are optional and may only be specified if category is also specified.
                    * Review predicates accept numbers between 0 and 5 and are inclusive.
                    * When using either of the 'between' strings to construct a targeting expression the format of the string is 'double-double' where the first double must be smaller than the second double. Prices are not inclusive.
                    * The exactProduct, similarProduct, and negative types do not utilize the value field.
                    * The only type currently applicable to Amazon Audiences targeting is 'audienceSameAs'.
                    * A 'relatedProduct' TargetingPredicateBase will Target an audience that has purchased a related product in the past 7,14,30,60,90,180, or 365 days.
                    * **Future** A 'negative' TargetingPredicateBase will exclude that TargetingPredicateNested from the overall audience.

                    | '**type**': *string*, {'enum': '[ asinCategorySameAs, asinBrandSameAs, asinPriceBetween, asinPriceGreaterThan, asinPriceLessThan, asinReviewRatingLessThan, asinReviewRatingGreaterThan, asinReviewRatingBetween, similarProduct, exactProduct, asinIsPrimeShippingEligible, asinAgeRangeSameAs, asinGenreSameAs, audienceSameAs, lookback, negative, relatedProduct ]'}
                    | '**value**': *string*, {'description': 'The value to be targeted. example: B0123456789'}

            Returns:

            ApiResponse


        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sd/targets', method='PUT')
    def edit_products_targets(self, **kwargs) -> ApiResponse:
        r"""
        Updates one or more targeting clauses. Targeting clauses are identified using their targetId. The mutable fields are bid and state. Maximum length of the array is 100 objects.

        body: | REQUIRED {'description': 'A list of up to 100 targeting clauses. Mutable fields: state, bid.}'

            | '**state**': *string*, {'description': '[ enabled, paused, archived ]'}
            | '**bid**': *number($float)*, {'description': 'The bid will override the adGroup bid if specified. This field is not used for negative targeting clauses. The bid must be less than the maximum allowable bid for the campaign's marketplace; for a list of maximum allowable bids, find the "Bid constraints by marketplace" table in our documentation overview.'}
            | '**targetId**': *integer($int64)*, {'description': 'The target id.'}

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sd/targets/{}', method='GET')
    def get_products_target(self, targetId, **kwargs) -> ApiResponse:
        r"""
        This call returns the minimal set of targeting clause fields.

            path **targetId**:*number* | Required. The identifier of a targeting clause.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), targetId), params=kwargs)

    @sp_endpoint('/sd/targets/{}', method='DELETE')
    def delete_products_target(self, targetId, **kwargs) -> ApiResponse:
        r"""
        Equivalent to using the updateTargetingClauses operation to set the state property of a targeting clause to archived. See Developer Notes for more information.

            path **targetId**:*number* | Required. The identifier of a targeting clause.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), targetId), params=kwargs)

    @sp_endpoint('/sd/targets/extended', method='GET')
    def list_products_targets_extended(self, **kwargs) -> ApiResponse:
        r"""
        Gets an array of TargetingClauseEx objects for a set of requested targets. Note that this call returns the full set of targeting clause extended fields, but is less efficient than getTargets.

            | query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Defaults to 0.
            | query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.
            | query **stateFilter**:*string* | Optional. Optional. Restricts results to those with state set to values in the specified comma-separated list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived. Default value : enabled, paused, archived.
            | query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.
            | query **adGroupIdFilter**:*string* | Optional. Optional list of comma separated adGroupIds. Restricts results to targeting clauses with the specified adGroupId
            | query **targetIdFilter**:*string* | Optional. A comma-delimited list of target identifiers. Missing in Amazon documentation.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sd/targets/extended/{}', method='GET')
    def get_products_target_extended(self, targetId, **kwargs) -> ApiResponse:
        r"""
        Gets a targeting clause object with extended fields. Note that this call returns the full set of targeting clause extended fields, but is less efficient than getTarget.

            path **targetId**:*number* | Required. The identifier of a targeting clause.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), targetId), params=kwargs)
