from ad_api.base import Client, sp_endpoint, ApiResponse


class CampaignsRecommendations(Client):
    @sp_endpoint('/sp/campaign/recommendations', method='GET')
    def list_campaigns_recommendations(self, **kwargs) -> ApiResponse:
        r"""
        Gets the top consolidated recommendations across bid, budget, targeting for SP campaigns given an advertiser profile id. The recommendations are refreshed everyday.

        Param:
            | query **nextToken** (string). Optional. Token to retrieve subsequent page of results.
            | query **maxResults** (string). Optional. Limits the number of items to return in the response.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)
