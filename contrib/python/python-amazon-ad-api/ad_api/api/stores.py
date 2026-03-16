from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class Stores(Client):
    """
    Brand Metrics provides a new measurement solution that quantifies opportunities for your brand at each stage of the customer journey on Amazon, and helps brands understand the value of different shopping engagements that impact stages of that journey. You can now access Awareness and Consideration indices that compare your performance to peers using models predictive of consideration and sales. Brand Metrics quantifies the number of customers in the awareness and consideration marketing funnel stages and is built at scale to measure all shopping engagements with your brand on Amazon, not just ad-attributed engagements. Additionally, BM breaks out key shopping engagements at each stage of the shopping journey, along with the Return on Engagement, so you can measure the historical sales following a consideration event or purchase.
    """

    @sp_endpoint('/stores/{}/asinMetrics', method='POST')
    def get_asin_engagement_for_store(self, brandEntityId, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        getAsinEngagementForStore
        """
        content_type = 'application/vnd.GetAsinEngagementForStoreRequest.v'+ str(version) +'+json'
        accept = 'application/vnd.GetAsinEngagementForStoreResponse.v'+ str(version) +'+json'
        headers = {'Content-Type': content_type, 'Accept': accept}
        return self._request(fill_query_params(kwargs.pop('path'), brandEntityId), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)

    @sp_endpoint('/stores/{}/insights', method='GET')
    def get_insights_for_store_api(self, brandEntityId, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        getInsightsForStoreAPI
        """
        content_type = 'application/vnd.GetInsightsForStoreRequest.v'+ str(version) +'+json'
        # accept = 'application/vnd.GetInsightsForStoreResponse.v'+ str(version) +'+json'
        # headers = {'Content-Type': content_type, 'Accept': accept}
        return self._request(fill_query_params(kwargs.pop('path'), brandEntityId),
                             data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs)
        # return self._request(fill_query_params(kwargs.pop('path'), brandEntityId), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
        
        

