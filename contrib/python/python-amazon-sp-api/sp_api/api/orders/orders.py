from sp_api.base import sp_endpoint, fill_query_params, ApiResponse, deprecated
from sp_api.base import Client, Marketplaces


class Orders(Client):
    """
    :link: https://github.com/amzn/selling-partner-api-docs/tree/main/references/orders-api
    """

    @sp_endpoint('/orders/v0/orders')
    def get_orders(self, **kwargs) -> ApiResponse:
        """
        get_orders(self, **kwargs) -> ApiResponse
        Returns orders created or updated during the time frame indicated by the specified parameters.
        You can also apply a range of filtering criteria to narrow the list of orders returned.
        If NextToken is present, that will be used to retrieve the orders instead of other criteria.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============


        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                Orders().get_orders(CreatedAfter='TEST_CASE_200', MarketplaceIds=["ATVPDKIKX0DER"])

        Args:
            key CreatedAfter: date
            key CreatedBefore: date
            key LastUpdatedAfter: date
            key LastUpdatedBefore: date
            key OrderStatuses: [str]
            key MarketplaceIds: [str]
            key FulfillmentChannels: [str]
            key PaymentMethods: [str]
            key BuyerEmail: str
            key SellerOrderId: str
            key MaxResultsPerPage: int
            key EasyShipShipmentStatuses: [str]
            key NextToken: str
            key AmazonOrderIds: [str]
            key RestrictedResources: [str]

        Returns:
            ApiResponse:


        """
        if 'RestrictedResources' in kwargs:
            return self._access_restricted(kwargs)
        return self._request(kwargs.pop('path'), params={**kwargs})

    @sp_endpoint('/orders/v0/orders/{}')
    def get_order(self, order_id: str, **kwargs) -> ApiResponse:
        """
        get_order(self, order_id: str, **kwargs) -> ApiResponse
        Returns the order indicated by the specified order ID.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============


        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                Orders().get_order('TEST_CASE_200')

        Args:
            order_id: str
            key RestrictedResources: [str]
            **kwargs:

        Returns:
            ApiResponse:


        """
        if 'RestrictedResources' in kwargs:
            kwargs.update({'original_path': fill_query_params(kwargs.get('path'), order_id)})
            return self._access_restricted(kwargs)
        return self._request(fill_query_params(kwargs.pop('path'), order_id), params={**kwargs}, add_marketplace=False)

    @sp_endpoint('/orders/v0/orders/{}/orderItems')
    def get_order_items(self, order_id: str, **kwargs) -> ApiResponse:
        """
        get_order_items(self, order_id: str, **kwargs) -> ApiResponse

        Returns detailed order item information for the order indicated by the specified order ID.
        If NextToken is provided, it's used to retrieve the next page of order items.

        Note: When an order is in the Pending state (the order has been placed but payment has not been authorized),
        the getOrderItems operation does not return information about pricing, taxes, shipping charges, gift status or
        promotions for the order items in the order.
        After an order leaves the Pending state (this occurs when payment has been authorized) and enters the Unshipped,
        Partially Shipped, or Shipped state, the getOrderItems operation returns information about pricing, taxes,
        shipping charges, gift status and promotions for the order items in the order.


        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============



        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                Orders().get_order_items('TEST_CASE_200')

        Args:
            order_id: str
            key RestrictedResources: [str]
            **kwargs:

        Returns:
            ApiResponse:

        """
        if 'RestrictedResources' in kwargs:
            kwargs.update({'original_path': fill_query_params(kwargs.get('path'), order_id)})
            return self._access_restricted(kwargs)
        return self._request(fill_query_params(kwargs.pop('path'), order_id), params={**kwargs})

    @sp_endpoint('/orders/v0/orders/{}/address')
    def get_order_address(self, order_id, **kwargs) -> ApiResponse:
        """
        get_order_address(self, order_id, **kwargs) -> ApiResponse

        Returns the shipping address for the order indicated by the specified order ID.

        :note: To get useful information from this method, you need to have access to PII.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============

        Examples:
            Orders().get_order_address('TEST_CASE_200')

        Args:
            order_id: str
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), order_id), params={**kwargs})

    @sp_endpoint('/orders/v0/orders/{}/buyerInfo')
    def get_order_buyer_info(self, order_id: str, **kwargs) -> ApiResponse:
        """
        get_order_buyer_info(self, order_id: str, **kwargs) -> ApiResponse
        Returns buyer information for the order indicated by the specified order ID.

        :note: To get useful information from this method, you need to have access to PII.


        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============


        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            Orders().get_order_buyer_info('TEST_CASE_200')

        Args:
            order_id: str
            **kwargs:

        Returns:
            GetOrderBuyerInfoResponse:

        """
        return self._request(fill_query_params(kwargs.pop('path'), order_id), params={**kwargs})

    @sp_endpoint('/orders/v0/orders/{}/orderItems/buyerInfo')
    def get_order_items_buyer_info(self, order_id: str, **kwargs) -> ApiResponse:
        """
        get_order_items_buyer_info(self, order_id: str, **kwargs) -> ApiResponse

        Returns buyer information in the order items of the order indicated by the specified order ID.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                Orders().get_order_items_buyer_info('TEST_CASE_200')

        Args:
            order_id: str
            key NextToken: str | retrieve data by next token

        Returns:
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), order_id), params=kwargs)

    @sp_endpoint('/orders/v0/orders/{}/shipment', method='POST')
    def update_shipment_status(self, order_id: str, **kwargs) -> ApiResponse:
        """
        update_shipment_status(self, order_id: str, **kwargs) -> ApiResponse
        Update the shipment status.
        **Usage Plan:**
        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       15
        ======================================  ==============
        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.
        Examples:
            literal blocks::
                Orders().update_shipment_status(
                    order_id='123-1234567-1234567',
                    marketplaceId='ATVPDKIKX0DER',
                    shipmentStatus='ReadyForPickup'
                )
        Args:
            order_id: str
        Returns:
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), order_id), res_no_data=True, data=kwargs)

    @sp_endpoint('/orders/v0/orders/{}/shipmentConfirmation', method='POST')
    def confirm_shipment(self, order_id: str, **kwargs) -> ApiResponse:
        """
        confirm_shipment(self, order_id: str, **kwargs) -> ApiResponse
        Updates the shipment confirmation status for a specified order.
        **Usage Plan:**
        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       10
        ======================================  ==============
        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.
        Examples:
            literal blocks::
                Orders().confirm_shipment(
                    order_id='123-1234567-1234567',
                    marketplaceId='ATVPDKIKX0DER',
                    packageDetail={
                        'packageReferenceId': '0001',
                        'carrierCode': 'DHL',
                        "shippingMethod": 'Paket',
                        'trackingNumber': '1234567890',
                        'shipDate': '2023-03-19T12:00:00Z',
                        'orderItems': [
                            {
                                'orderItemId': '123456789',
                                'quantity': 1
                            },
                            {
                                'orderItemId': '2345678901',
                                'quantity': 2
                            },
                        ]
                    }
                )
        Args:
            order_id: str
        Returns:
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), order_id), add_marketplace=False, res_no_data=True,
                             data=kwargs)

    @sp_endpoint('/tokens/2021-03-01/restrictedDataToken', method='POST')
    def _get_token(self, **kwargs):
        data_elements = kwargs.pop('RestrictedResources')

        restricted_resources = [{
            "method": "GET",
            "path": kwargs.get('original_path'),
            "dataElements": data_elements
        }]

        return self._request(kwargs.pop('path'), data={'restrictedResources': restricted_resources, **kwargs})

    def _access_restricted(self, kwargs):
        if 'original_path' not in kwargs:
            kwargs.update({'original_path': kwargs['path']})
        token = self._get_token(**kwargs).payload
        self.restricted_data_token = token['restrictedDataToken']
        r = self._request(kwargs.pop('original_path'), params={**kwargs})
        if not self.keep_restricted_data_token:
            self.restricted_data_token = None
        return r
