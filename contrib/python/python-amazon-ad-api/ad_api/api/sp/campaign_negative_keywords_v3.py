from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class CampaignNegativeKeywordsV3(Client):
    @sp_endpoint('/sp/campaignNegativeKeywords/list', method='POST')
    def list_campaign_negative_keywords(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        List Campaign negative keywords

        Request Body
            | SponsoredProductsListSponsoredProductsCampaignNegativeKeywordsRequestContent {
            | **campaignIdFilter**\* SponsoredProductsReducedObjectIdFilter {
            |   Filter entities by the list of objectIds
            |   **include**\* (array). minItems: 0. maxItems: 1000 [
            |       entity object identifier (string)
            |   ]
            |   }
            | **campaignNegativeKeywordIdFilter**\* SponsoredProductsObjectIdFilter {
            |   Filter entities by the list of objectIds
            |   **include**\* (array). minItems: 0. maxItems: 1000 [
            |       entity object identifier (string)
            |   ]
            |   }
            | **maxResults** integer($int32) Number of records to include in the paginated response. Defaults to max page size for given API
            | **nextToken** (string) token value allowing to navigate to the next response page
            | **includeExtendedDataFields** (boolean) Whether to get entity with extended data fields such as creationDate, lastUpdateDate, servingStatus. Enum: [ BROAD_MATCH, EXACT_MATCH ]
            | **campaignNegativeKeywordTextFilter**\* SponsoredProductsKeywordTextFilter {
            |   Filter by keywordText
            |   **queryTermMatchType** SponsoredProductsQueryTermMatchType (string): Match type for query filters.
            |   **include** (array). minItems: 0. maxItems: 1000 [
            |       entity object identifier (string)
            |   ]
            |   }
            | **matchTypeFilter**\* (array). [
            |   Restricts results to resources with the selected matchType.
            |       SponsoredProductsNegativeMatchType (string) Enum: ['NEGATIVE_EXACT', 'NEGATIVE_PHRASE', 'NEGATIVE_BROAD', 'OTHER']
            |   ]
            | }
        Returns
            ApiResponse
        """
        schema_version = 'application/vnd.spCampaignNegativeKeyword.v' + str(version) + '+json'
        headers = {"Accept": schema_version, "Content-Type": schema_version}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/campaignNegativeKeywords/delete', method='POST')
    def delete_campaign_negative_keyword(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        Delete Campaign negative keywords

        Request Body
            | SponsoredProductsDeleteSponsoredProductsCampaignNegativeKeywordsRequestContent {
            | **campaignNegativeKeywordIdFilter**\* SponsoredProductsObjectIdFilter {
            |   Filter entities by the list of objectIds
            |   **include**\* (array). minItems: 0. maxItems: 1000 [
            |       entity object identifier (string)
            |   ]
            |   }
            | }
        Returns
            ApiResponse
        """
        schema_version = 'application/vnd.spCampaignNegativeKeyword.v' + str(version) + '+json'
        headers = {"Accept": schema_version, "Content-Type": schema_version}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/campaignNegativeKeywords', method='POST')
    def create_campaign_negative_keywords(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Create Campaign negative keywords

        | header **Prefer**:*boolean* | Used to indicate the behavior preferred by the client but is not required for successful completion of the request. Supported values will be updated in the future.

        Request Body
            | SponsoredProductsCreateSponsoredProductsCampaignNegativeKeywordsRequestContent {
            | **campaignNegativeKeywords**\* (array) minItems: 0 maxItems: 1000 [
            |   SponsoredProductsCreateCampaignNegativeKeyword {
            |       **campaignId**\* (string): The identifier of the campaign to which the keyword is associated.
            |       **matchType**\* SponsoredProductsCreateOrUpdateNegativeMatchType (string) Enum: ['NEGATIVE_EXACT', 'NEGATIVE_PHRASE', 'NEGATIVE_BROAD']
            |       **state**\* SponsoredProductsCreateOrUpdateEntityState (string): Entity state for create or update operation Enum: ['ENABLED', 'PAUSED']
            |       **keywordText**\* (string): The keyword text.
            |       }
            |   ]
            | }
        Returns
            ApiResponse
        """
        schema_version = 'application/vnd.spCampaignNegativeKeyword.v' + str(version) + '+json'
        headers = {"Accept": schema_version, "Content-Type": schema_version}
        prefer_value = 'return=representation'
        if prefer:
            headers.update({"Prefer": prefer_value})
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/sp/campaignNegativeKeywords', method='PUT')
    def edit_campaign_negative_keywords(self, version: int = 3, prefer: bool = False, **kwargs) -> ApiResponse:
        r"""
        Update Campaign negative keywords

        | header **Prefer**:*boolean* | Used to indicate the behavior preferred by the client but is not required for successful completion of the request. Supported values will be updated in the future.

        Request Body
            | SponsoredProductsUpdateSponsoredProductsCampaignNegativeKeywordsRequestContent {
            | **campaignNegativeKeywords**\* (array) minItems: 0 maxItems: 1000 [
            |   SponsoredProductsUpdateCampaignNegativeKeyword {
            |       **keywordId**\* (string): entity object identifier.
            |       **state**\* SponsoredProductsCreateOrUpdateEntityState (string): Entity state for create or update operation Enum: ['ENABLED', 'PAUSED']
            |       }
            |   ]
            | }
        Returns
            ApiResponse
        """
        schema_version = 'application/vnd.spCampaignNegativeKeyword.v' + str(version) + '+json'
        headers = {"Accept": schema_version, "Content-Type": schema_version}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
