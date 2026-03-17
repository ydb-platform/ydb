from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class Recommendations(Client):
    @sp_endpoint('/sd/recommendations/creative/headline', method='POST')
    def list_headline_recommendations(self, version: float = 4.0, **kwargs) -> ApiResponse:
        r"""
        You can use this Sponsored Display API to retrieve creative headline recommendations from an array of ASINs.

        Request Body
            | SDHeadlineRecommendationRequest {
            | Request structure of SD headline recommendation API.
            |   **asins** (array) minItems: 0 maxItems: 100 [
            |       (string) An array of ASINs associated with the creative.
            |   ]
            |   **maxNumRecommendations** (array) Maximum number of recommendations that API should return. Response will [0, maxNumRecommendations] recommendations (recommendations are not guaranteed as there can be instances where the ML model can not generate policy compliant headlines for the given set of asins). maximum: 10. minimum: 1 .
            |   **adFormat** (string): Enum: [ SPONSORED_DISPLAY ]
            | }
        Returns
            ApiResponse
        """
        schema = 'application/vnd.sdheadlinerecommendationrequest.v' + str(version) + "+json"
        headers = {'Content-Type': schema}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
