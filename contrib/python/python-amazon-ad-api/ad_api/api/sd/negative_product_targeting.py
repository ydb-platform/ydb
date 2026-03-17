from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class NegativeTargets(Client):
    """Amazon Advertising API for Sponsored Display

    Documentation: https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Negative%20targeting

    This API enables programmatic access for campaign creation, management, and reporting for Sponsored Display campaigns. For more information on the functionality, see the `Sponsored Display Support Center <https://advertising.amazon.com/help#GTPPHE6RAWC2C4LZ>`_ . For API onboarding information, see the `account setup <https://advertising.amazon.com/API/docs/en-us/setting-up/account-setup>`_  topic.

    This specification is available for download from the `Advertising API developer portal <https://d3a0d0y2hgofx6.cloudfront.net/openapi/en-us/sponsored-display/3-0/openapi.yaml>`_.

    """

    @sp_endpoint('/sd/negativeTargets', method='GET')
    def list_negative_targets(self, **kwargs) -> ApiResponse:
        r"""
        list_negative_targets(self, \*\*kwargs) -> ApiResponse

        Gets a list of negative targeting clauses filtered by specified criteria.

            | query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0
            | query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.
            | query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.
            | query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.
            | query **adGroupIdFilter**:*string* | Optional. Restricts results to keywords associated with ad groups specified by identifier in the comma-delimited list.
            | query **targetIdFilter**:*string* | Optional. A comma-delimited list of target identifiers. Missing in official Amazon documentation

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sd/negativeTargets', method='PUT')
    def edit_negative_targets(self, **kwargs) -> ApiResponse:
        r"""
        Updates one or more negative targeting clauses. Negative targeting clauses are identified using their targetId. The mutable field is state. Maximum length of the array is 100 objects.

        body: | UpdateNegativeTargetingClause REQUIRED {'description': 'A list of up to 100 negative targeting clauses. Note that the only mutable field is state.}'

            | '**state**': *number*, {'description': 'The resource state. [ enabled, paused, archived ]'}
            | '**targetId***': *integer($int64)*, {'description': 'The identifier of the TargetId.'}

        Returns:

            ApiResponse


        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sd/negativeTargets', method='POST')
    def create_negative_targets(self, **kwargs) -> ApiResponse:
        r"""
        create_products_targets(self, \*\*kwargs) -> ApiResponse:

        Creates one or more targeting expressions.

        body: | REQUIRED {'description': 'An array of asins objects.}'

            | '**state**': *number*, {'description': 'The current resource state. [ enabled, paused, archived ]'}
            | '**adGroupId**': *number*, {'description': 'The identifier of the ad group to which this negative target is associated.'}
            | '**expression**'
            |       '**type**': *string*, {'description': 'The intent type. See the targeting topic in the Amazon Advertising support center for more information.', 'enum': '[ asinSameAs, asinBrandSameAs ]'}
            |       '**value**': *string*, {'description': 'The value to be negatively targeted. Used only in manual expressions.'}
            | '**expressionType**': *string*, {'description': '[ auto, manual ]'}

        Returns:

            ApiResponse


        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sd/negativeTargets/{}', method='GET')
    def get_negative_target(self, targetId, **kwargs) -> ApiResponse:
        r"""

        This call returns the minimal set of negative targeting clause fields, but is more efficient than getNegativeTargetsEx.

        Get a negative targeting clause specified by identifier.

            path **negativeTargetId**:*integer* | Required. The negative targeting clause identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), targetId), params=kwargs)

    @sp_endpoint('/sd/negativeTargets/{}', method='DELETE')
    def delete_negative_targets(self, targetId, **kwargs) -> ApiResponse:
        r"""

        Equivalent to using the updateNegativeTargetingClauses operation to set the state property of a targeting clause to archived. See Developer Notes for more information.

        Archives a negative targeting clause.

            path **negativeTargetId**:*integer* | Required. The negative targeting clause identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), targetId), params=kwargs)

    @sp_endpoint('/sd/negativeTargets/extended', method='GET')
    def list_negative_targets_extended(self, **kwargs) -> ApiResponse:
        r"""
        Gets an array of NegativeTargetingClauseEx objects for a set of requested negative targets. Note that this call returns the full set of negative targeting clause extended fields, but is less efficient than getNegativeTargets.

            | query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0
            | query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.
            | query **stateFilter**:*string* | Optional. The returned array is filtered to include only ad groups with state set to one of the values in the specified comma-delimited list. Available values : enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived Default value : enabled, paused, archived.
            | query **campaignIdFilter**:*string* | Optional. A comma-delimited list of campaign identifiers.
            | query **adGroupIdFilter**:*string* | Optional. Restricts results to keywords associated with ad groups specified by identifier in the comma-delimited list.
            | query **targetIdFilter**:*string* | Optional. A comma-delimited list of target identifiers. Missing in official Amazon documentation

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sd/negativeTargets/extended/{}', method='GET')
    def get_negative_target_extended(self, targetId, **kwargs) -> ApiResponse:
        r"""
        Gets a negative targeting clause with extended fields. Note that this call returns the full set of negative targeting clause extended fields, but is less efficient than getNegativeTarget.

            path **negativeTargetId**:*integer* | Required. The negative targeting clause identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), targetId), params=kwargs)
