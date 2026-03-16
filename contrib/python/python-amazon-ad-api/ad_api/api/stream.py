from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class Stream(Client):
    """Amazon Marketing Stream

    Documentation: https://advertising.amazon.com/API/docs/en-us/amazon-marketing-stream/openapi
    """

    @sp_endpoint('/streams/subscriptions', method='POST')
    def create_subscription(self, **kwargs) -> ApiResponse:
        r"""
        Request the creation a new subscription

        Request body
            | **notes** (string): [optional] Additional details associated with the subscription
            | **clientRequestToken** (string): [required] Unique value supplied by the caller used to track identical API requests. Should request be re-tried, the caller should supply the same value.
            | **dataSetId** (string): [required] Identifier of data set, callers can be subscribed to. Please refer to https://advertising.amazon.com/API/docs/en-us/amazon-marketing-stream/data-guide for the list of all data sets.
            | **destinationArn** (string): [required] AWS ARN of the destination endpoint associated with the subscription. Supported destination type - SQS.

        Returns:
            ApiResponse
        """
        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs)

    @sp_endpoint('/streams/subscriptions/{}', method='PUT')
    def update_subscription(self, subscription_id: str, **kwargs) -> ApiResponse:
        r"""
        Update an existing subscription

        Request body
            | **notes** (string): [optional] Additional details associated with the subscription
            | **status** (string): [optional] Update the status of the entity. Supported value: 'ARCHIVED'

        Returns:
            ApiResponse
        """
        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(fill_query_params(kwargs.pop('path'), subscription_id), data=body, params=kwargs)

    @sp_endpoint('/streams/subscriptions/{}', method='GET')
    def get_subscription(self, subscription_id: str, **kwargs) -> ApiResponse:
        r"""
        Fetch a specific subscription by ID

        Keyword Args
            | path **subscriptionId** (string): [required] Unique subscription identifier

        Returns:
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), subscription_id), params=kwargs)

    @sp_endpoint('/streams/subscriptions', method='GET')
    def list_subscriptions(self, **kwargs) -> ApiResponse:
        r"""
        List subscriptions

            query **maxResults**:*string* | Optional. [1-5000] Desired number of entries in the response, defaults to maximum value

            query **startingToken**:*string* | Optional. Token which can be used to get the next page of results, if more entries exist

        Returns:
            ApiResponse
        """
        return self._request(kwargs.pop('path'), params=kwargs)
