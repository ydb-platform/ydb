from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class Insights(Client):
    """ """

    @sp_endpoint('/insights/audiences/{}/overlappingAudiences', method='GET')
    def get_insights(self, audienceId: str, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        get_insights(audienceId: str, version: int = 1, **kwargs) -> ApiResponse

        Retrieves the top audiences that overlap with the provided audience.

        Requires one of these permissions: ["advertiser_campaign_edit","advertiser_campaign_view"]

        path **audienceId**:string | Required. The identifier of an audience.

        internal **version**:int | Optional. The version of the overlapping audiences accept 'application/vnd.insightsaudiencesoverlap.v'+str(version)+'+json'. Available values : 1, 2 Default: 1

        query **adType**:string | Required. The advertising program. Available values : DSP, SD

        query **advertiserId**:string | Optional. The identifier of the advertiser you'd like to retrieve overlapping audiences for. This parameter is required for the DSP adType, but is optional for the SD adType.

        query **minimumAudienceSize**:number | Optional. If specified, the sizes of all returned overlapping audiences will be at least the provided size. This parameter is supported only for request to return application/vnd.insightsaudiencesoverlap.v1+json.

        query **maximumAudienceSize**:number | Optional. If specified, the sizes of all returned overlapping audiences will be at most the provided size. This parameter is supported only for request to return application/vnd.insightsaudiencesoverlap.v1+json.

        query **minimumOverlapAffinity**:number | Optional. If specified, the affinities of all returned overlapping audiences will be at least the provided affinity.

        query **maximumOverlapAffinity**:number | Optional. If specified, the affinities of all returned overlapping audiences will be at most the provided affinity.

        query **audienceCategory**:array[string] | Optional. f specified, the categories of all returned overlapping audiences will be one of the provided categories.

        query **maxResults**:integer | Optional. Sets the maximum number of overlapping audiences in the response. This parameter is supported only for request to return application/vnd.insightsaudiencesoverlap.v2+json. Default value : 30

        query **nextToken**:string | Optional. TToken to be used to request additional overlapping audiences. If not provided, the top 30 overlapping audiences are returned. Note: subsequent calls must be made using the same parameters as used in previous requests.

        """

        contentType = 'application/vnd.insightsaudiencesoverlap.v' + str(version) + '+json'
        headers = {'Accept': contentType}
        return self._request(fill_query_params(kwargs.pop('path'), audienceId), params=kwargs, headers=headers)
