import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class Messaging(Client):
    """
    Messaging SP-API Client
    :link: 

    With the Messaging API you can build applications that send messages to buyers. You can get a list of message types that are available for an order that you specify, then call an operation that sends a message to the buyer for that order. The Messaging API returns responses that are formed according to the <a href=https://tools.ietf.org/html/draft-kelly-json-hal-08>JSON Hypertext Application Language</a> (HAL) standard.
    """

    @sp_endpoint('/messaging/v1/orders/{}', method='GET')
    def get_messaging_actions_for_order(self, order_id, **kwargs) -> ApiResponse:
        """
        get_messaging_actions_for_order(self, order_id, **kwargs) -> ApiResponse

        Returns a list of message types that are available for an order that you specify. A message type is represented by an actions object, which contains a path and query parameter(s). You can use the path and parameter(s) to call an operation that sends a message.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       5
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            order_id:string | * REQUIRED An Amazon order identifier. This specifies the order for which you want a list of available message types.
            key marketplaceIds:array | * REQUIRED A marketplace identifier. This specifies the marketplace in which the order was placed. Only one marketplace can be specified.

        Returns:
            ApiResponse:
        """

        return self._request(fill_query_params(kwargs.pop('path'), order_id), params=kwargs)

    @sp_endpoint('/messaging/v1/orders/{}/messages/confirmCustomizationDetails', method='POST')
    def confirm_customization_details(self, order_id, **kwargs) -> ApiResponse:
        """
        confirm_customization_details(self, order_id, **kwargs) -> ApiResponse

        Sends a message asking a buyer to provide or verify customization details such as name spelling, images, initials, etc.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       5
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            order_id:string | * REQUIRED An Amazon order identifier. This specifies the order for which a message is sent.
            body: {
              "text": "string",
              "attachments": [
                {
                  "uploadDestinationId": "string",
                  "fileName": "string"
                }
              ]
            }

        Returns:
            ApiResponse:
        """

        return self._request(
            fill_query_params(kwargs.pop('path'), order_id), data=kwargs.pop("body"), params={"marketplaceIds": self.marketplace_id, "method": kwargs.pop('method')}
        )

    @sp_endpoint('/messaging/v1/orders/{}/messages/confirmDeliveryDetails', method='POST')
    def create_confirm_delivery_details(self, order_id, **kwargs) -> ApiResponse:
        """
        create_confirm_delivery_details(self, order_id, **kwargs) -> ApiResponse

        Sends a message to a buyer to arrange a delivery or to confirm contact information for making a delivery.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       5
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            order_id:string | * REQUIRED An Amazon order identifier. This specifies the order for which a message is sent.
            body: {
              "text": "string"
            }


        Returns:
            ApiResponse:
        """

        return self._request(
            fill_query_params(kwargs.pop('path'), order_id), data=kwargs.pop("body"), params={"marketplaceIds": self.marketplace_id, "method": kwargs.pop('method')}
        )

    @sp_endpoint('/messaging/v1/orders/{}/messages/legalDisclosure', method='POST')
    def create_legal_disclosure(self, order_id, **kwargs) -> ApiResponse:
        """
        create_legal_disclosure(self, order_id, **kwargs) -> ApiResponse

        Sends a critical message that contains documents that a seller is legally obligated to provide to the buyer. This message should only be used to deliver documents that are required by law.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       5
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            order_id:string | * REQUIRED An Amazon order identifier. This specifies the order for which a message is sent.
            body: {
              "attachments": [
                {
                  "uploadDestinationId": "string",
                  "fileName": "string"
                }
              ]
            }


        Returns:
            ApiResponse:
        """

        return self._request(
            fill_query_params(kwargs.pop('path'), order_id), data=kwargs.pop("body"), params={"marketplaceIds": self.marketplace_id, "method": kwargs.pop('method')}
        )

    @sp_endpoint('/messaging/v1/orders/{}/messages/negativeFeedbackRemoval', method='POST')
    def create_negative_feedback_removal(self, order_id, **kwargs) -> ApiResponse:
        """
        create_negative_feedback_removal(self, order_id, **kwargs) -> ApiResponse

        Sends a non-critical message that asks a buyer to remove their negative feedback. This message should only be sent after the seller has resolved the buyer's problem.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       5
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            order_id:string | * REQUIRED An Amazon order identifier. This specifies the order for which a message is sent.

        Returns:
            ApiResponse:
        """

        return self._request(
            fill_query_params(kwargs.pop('path'), order_id), data=kwargs.pop("body"), params={"marketplaceIds": self.marketplace_id, "method": kwargs.pop('method')}
        )

    @sp_endpoint('/messaging/v1/orders/{}/messages/confirmOrderDetails', method='POST')
    def create_confirm_order_details(self, order_id, **kwargs) -> ApiResponse:
        """
        create_confirm_order_details(self, order_id, **kwargs) -> ApiResponse

        Sends a message to ask a buyer an order-related question prior to shipping their order.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       5
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            order_id:string | * REQUIRED An Amazon order identifier. This specifies the order for which a message is sent.
            body:{
              "text": "string"
            }

        Returns:
            ApiResponse:
        """

        return self._request(
            fill_query_params(kwargs.pop('path'), order_id), data=kwargs.pop("body"), params={"marketplaceIds": self.marketplace_id, "method": kwargs.pop('method')}
        )

    @sp_endpoint('/messaging/v1/orders/{}/messages/confirmServiceDetails', method='POST')
    def create_confirm_service_details(self, order_id, **kwargs) -> ApiResponse:
        """
        create_confirm_service_details(self, order_id, **kwargs) -> ApiResponse

        Sends a message to contact a Home Service customer to arrange a service call or to gather information prior to a service call.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       5
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            order_id:string | * REQUIRED An Amazon order identifier. This specifies the order for which a message is sent.
            body: {
              "text": "string"
            }


        Returns:
            ApiResponse:
        """

        return self._request(
            fill_query_params(kwargs.pop('path'), order_id), data=kwargs.pop("body"), params={"marketplaceIds": self.marketplace_id, "method": kwargs.pop('method')}
        )

    @sp_endpoint('/messaging/v1/orders/{}/messages/amazonMotors', method='POST')
    def create_amazon_motors(self, order_id, **kwargs) -> ApiResponse:
        """
        create_amazon_motors(self, order_id, **kwargs) -> ApiResponse

        Sends a message to a buyer to provide details about an Amazon Motors order. This message can only be sent by Amazon Motors sellers.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       5
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            order_id:string | * REQUIRED An Amazon order identifier. This specifies the order for which a message is sent.
            body: {
              "attachments": [
                {
                  "uploadDestinationId": "string",
                  "fileName": "string"
                }
              ]
            }


        Returns:
            ApiResponse:
        """

        return self._request(
            fill_query_params(kwargs.pop('path'), order_id), data=kwargs.pop("body"), params={"marketplaceIds": self.marketplace_id, "method": kwargs.pop('method')}
        )

    @sp_endpoint('/messaging/v1/orders/{}/messages/warranty', method='POST')
    def create_warranty(self, order_id, **kwargs) -> ApiResponse:
        """
        create_warranty(self, order_id, **kwargs) -> ApiResponse

        Sends a message to a buyer to provide details about warranty information on a purchase in their order.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       5
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            order_id:string | * REQUIRED An Amazon order identifier. This specifies the order for which a message is sent.
            body: {
              "attachments": [
                {
                  "uploadDestinationId": "string",
                  "fileName": "string"
                }
              ],
              "coverageStartDate": "2019-08-24T14:15:22Z",
              "coverageEndDate": "2019-08-24T14:15:22Z"
            }


        Returns:
            ApiResponse:
        """

        return self._request(
            fill_query_params(kwargs.pop('path'), order_id), data=kwargs.pop("body"), params={"marketplaceIds": self.marketplace_id, "method": kwargs.pop('method')}
        )

    @sp_endpoint('/messaging/v1/orders/{}/attributes', method='GET')
    def get_attributes(self, order_id, **kwargs) -> ApiResponse:
        """
        get_attributes(self, order_id, **kwargs) -> ApiResponse

        Returns a response containing attributes related to an order. This includes buyer preferences.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       5
        ======================================  ==============

        Args:
            order_id:string | * REQUIRED An Amazon order identifier. This specifies the order for which a message is sent.

        Returns:
            ApiResponse:
        """

        return self._request(fill_query_params(kwargs.pop('path'), order_id), params=kwargs)

    @sp_endpoint('/messaging/v1/orders/{}/messages/digitalAccessKey', method='POST')
    def create_digital_access_key(self, order_id, **kwargs) -> ApiResponse:
        """
        create_digital_access_key(self, order_id, **kwargs) -> ApiResponse

        Sends a message to a buyer to share a digital access key needed to utilize digital content in their order.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       5
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            order_id:string | * REQUIRED An Amazon order identifier. This specifies the order for which a message is sent.
            body: {
              "text": "string",
              "attachments": [
                {
                  "uploadDestinationId": "string",
                  "fileName": "string"
                }
              ]
            }

        Returns:
            ApiResponse:
        """

        return self._request(
            fill_query_params(kwargs.pop('path'), order_id), data=kwargs.pop("body"), params={"marketplaceIds": self.marketplace_id, "method": kwargs.pop('method')}
        )

    @sp_endpoint('/messaging/v1/orders/{}/messages/unexpectedProblem', method='POST')
    def create_unexpected_problem(self, order_id, **kwargs) -> ApiResponse:
        """
        create_unexpected_problem(self, order_id, **kwargs) -> ApiResponse

        Sends a critical message to a buyer that an unexpected problem was encountered affecting the completion of the order.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       5
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            order_id:string | * REQUIRED An Amazon order identifier. This specifies the order for which a message is sent.
            body: {
              "text": "string"
            }

        Returns:
            ApiResponse:
        """

        return self._request(
            fill_query_params(kwargs.pop('path'), order_id), data=kwargs.pop("body"), params={"marketplaceIds": self.marketplace_id, "method": kwargs.pop('method')}
        )
