from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class CampaignNegativeTargets(Client):
    @sp_endpoint('/sp/campaignNegativeTargets/delete', method='POST')
    def delete_campaign_negative_targets(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        Deleting campaign negative targets.

        Request Body (required)
            | SponsoredProductsDeleteSponsoredProductsCampaignNegativeTargetingClausesRequestContent {
            | campaignNegativeTargetIdFilter* SponsoredProductsObjectIdFilter {
            | Filter entities by the list of objectIds
            |       **include**\* (array) minItems: 0 maxItems: 1000 [
            |           string entity object identifier
            |       ]
            |    }
            | }

        Returns
            ApiResponse
        """
        json_version = 'application/vnd.spcampaignNegativeTargetingClause.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/campaignNegativeTargets', method='POST')
    def create_campaign_negative_targets(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Create campaign negative targets.

        Param:

            | header **Prefer**\* (string). The "Prefer" header, as defined in [RFC7240], allows clients to request certain behavior from the service. The service ignores preference values that are either not supported or not known by the service. Either multiple Prefer headers are passed or single one with comma separated values, both forms are equivalent Supported preferences: return=representation - return the full object when doing create/update/delete operations instead of ids

        Request Body (required)
            | SponsoredProductsCreateSponsoredProductsCampaignNegativeTargetingClausesRequestContent {
            | **campaignNegativeTargetingClauses**\* SponsoredProductsCreateCampaignNegativeTargetingClause {
            |       **expression**\* (array) The NegativeTargeting expression. minItems: 0 maxItems: 1000 [
            |           SponsoredProductsCreateOrUpdateNegativeTargetingExpressionPredicate {
            |               **type**\* (string) SponsoredProductsCreateOrUpdateNegativeTargetingExpressionPredicateType. The type of negative targeting expression. Enum: ['ASIN_BRAND_SAME_AS', 'ASIN_SAME_AS'].
            |               **value** (string): The expression value
            |           }
            |       ]
            |       **campaignId**\* (string) The identifier of the campaign to which this target is associated.
            |       **state**\* (string) SponsoredProductsCreateOrUpdateEntityState. Entity state for create or update operation. Enum: [ ENABLED, PAUSED ]
            |    }
            | }
        Request Body (optional)

        Returns
            ApiResponse
        """
        json_version = 'application/vnd.spCampaignNegativeTargetingClause.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}
        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/campaignNegativeTargets', method='PUT')
    def edit_negative_product_targets(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Update campaign negative targets.

        Param:

            | header **Prefer**\* (string). The "Prefer" header, as defined in [RFC7240], allows clients to request certain behavior from the service. The service ignores preference values that are either not supported or not known by the service. Either multiple Prefer headers are passed or single one with comma separated values, both forms are equivalent Supported preferences: return=representation - return the full object when doing create/update/delete operations instead of ids

        Request Body (required)
            | SponsoredProductsUpdateSponsoredProductsCampaignNegativeTargetingClausesRequestContent {
            | **campaignNegativeTargetingClauses**\* SponsoredProductsUpdateCampaignNegativeTargetingClause {
            |       **expression**\* (array) The NegativeTargeting expression. minItems: 0 maxItems: 1000 [
            |           SponsoredProductsCreateOrUpdateNegativeTargetingExpressionPredicate {
            |               **type**\* (string) SponsoredProductsCreateOrUpdateNegativeTargetingExpressionPredicateType. The type of negative targeting expression. Enum: ['ASIN_BRAND_SAME_AS', 'ASIN_SAME_AS'].
            |               **value** (string): The expression value
            |           }
            |       ]
            |       **targetId**\* (string) The target identifier
            |       **state**\* (string) SponsoredProductsCreateOrUpdateEntityState. Entity state for create or update operation. Enum: [ ENABLED, PAUSED ]
            |    }
            | }
        Request Body (optional)

        Returns
            ApiResponse
        """
        json_version = 'application/vnd.spNegativeTargetingClause.v' + str(version) + "+json"
        headers = {"Accept": json_version, "Content-Type": json_version}

        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/campaignNegativeTargets/list', method='POST')
    def list_campaign_negative_targets(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        List campaign negative targets.

        Request Body (required)
            | SponsoredProductsListSponsoredProductsCampaignNegativeTargetingClausesRequestContent {
            | **campaignIdFilter** SponsoredProductsReducedObjectIdFilter {
            |       Filter entities by the list of objectIds
            |       **include**\* (array) minItems: 0 maxItems: 100 [
            |           string entity object identifier
            |       ]
            |    }
            | **campaignNegativeTargetIdFilter** SponsoredProductsObjectIdFilter {
            |       Filter entities by the list of objectIds
            |       **include**\* (array) minItems: 0 maxItems: 1000 [
            |           string entity object identifier
            |       ]
            |    }
            | **stateFilter** SponsoredProductsEntityStateFilter {
            |       Filter entities by state
            |       **include**\* (array) minItems: 0 maxItems: 10 [
            |           SponsoredProductsEntityState (string) The current resource state. Enum: [ ENABLED, PAUSED, ARCHIVED, ENABLING, USER_DELETED, OTHER ]
            |       ]
            |    }
            | **maxResults** integer($int32) Number of records to include in the paginated response. Defaults to max page size for given API
            | **nextToken** (string) token value allowing to navigate to the next response page
            | **asinFilter** SponsoredProductsAsinFilter {
            |       **queryTermMatchType** SponsoredProductsQueryTermMatchType (string): Match type for query filters. Enum: [ BROAD_MATCH, EXACT_MATCH ]
            |       **include** (array) maxItems: 100 [
            |           string
            |       ]
            |    }
            | **includeExtendedDataFields** (boolean) Whether to get entity with extended data fields such as creationDate, lastUpdateDate, servingStatus
            | }
        Request Body (optional)

        Returns
            ApiResponse
        """
        content_type = 'application/vnd.spcampaignNegativeTargetingClause.v' + str(version) + '+json'
        accept = 'application/vnd.spCampaignNegativeTargetingClause.v' + str(version) + '+json'
        headers = {"Accept": accept, "Content-Type": content_type}

        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
