
from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class MerchantFulfillment(Client):
    """
        :link: https://github.com/amzn/selling-partner-api-docs/blob/main/references/merchant-fulfillment-api/merchantFulfillmentV0.md

    """

    @sp_endpoint("/mfn/v0/eligibleServices", method='POST')
    def get_eligible_shipment_services_old(self, shipment_request_details: dict, **kwargs) -> ApiResponse:
        """
        get_eligible_shipment_services_old(self, shipment_request_details: dict, **kwargs) -> ApiResponse
        Returns a list of shipping service offers that satisfy the specified shipment request details.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                MerchantFulfillment().get_eligible_shipment_services_old({
                        "AmazonOrderId": "903-5563053-5647845",
                        "ItemList": [
                            {
                                "OrderItemId": "52986411826454",
                                "Quantity": 1
                            }
                        ],
                        "ShipFromAddress": {
                            "Name": "John Doe",
                            "AddressLine1": "300 Turnbull Ave",
                            "Email": "jdoeasdfllkj@yahoo.com",
                            "City": "Detroit",
                            "StateOrProvinceCode": "MI",
                            "PostalCode": "48123",
                            "CountryCode": "US",
                            "Phone": "7132341234"
                        },
                        "PackageDimensions": {
                            "Length": 10,
                            "Width": 10,
                            "Height": 10,
                            "Unit": "inches"
                        },
                        "Weight": {
                            "Value": 10,
                            "Unit": "oz"
                        },
                        "ShippingServiceOptions": {
                            "DeliveryExperience": "NoTracking",
                            "CarrierWillPickUp": False,
                            "CarrierWillPickUpOption": "ShipperWillDropOff"
                        }
                    })

        Args:
            shipment_request_details: dict:

        Returns:
            GetEligibleShipmentServicesResponse:
        """
        # GetEligibleShipmentServicesRequest
        data = {
            "ShipmentRequestDetails": shipment_request_details,
            **kwargs
        }

        return self._request(kwargs.pop('path'), data=data)

    @sp_endpoint("/mfn/v0/eligibleShippingServices", method='POST')
    def get_eligible_shipment_services(self, shipment_request_details: dict, **kwargs) -> ApiResponse:
        """
        get_eligible_shipment_services(self, shipment_request_details: dict, **kwargs) -> ApiResponse
        Returns a list of shipping service offers that satisfy the specified shipment request details.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                MerchantFulfillment().get_eligible_shipment_services({
                        "AmazonOrderId": "903-5563053-5647845",
                        "ItemList": [
                            {
                                "OrderItemId": "52986411826454",
                                "Quantity": 1
                            }
                        ],
                        "ShipFromAddress": {
                            "Name": "John Doe",
                            "AddressLine1": "300 Turnbull Ave",
                            "Email": "jdoeasdfllkj@yahoo.com",
                            "City": "Detroit",
                            "StateOrProvinceCode": "MI",
                            "PostalCode": "48123",
                            "CountryCode": "US",
                            "Phone": "7132341234"
                        },
                        "PackageDimensions": {
                            "Length": 10,
                            "Width": 10,
                            "Height": 10,
                            "Unit": "inches"
                        },
                        "Weight": {
                            "Value": 10,
                            "Unit": "oz"
                        },
                        "ShippingServiceOptions": {
                            "DeliveryExperience": "NoTracking",
                            "CarrierWillPickUp": False,
                            "CarrierWillPickUpOption": "ShipperWillDropOff"
                        }
                    })

        Args:
            shipment_request_details: dict:

        Returns:
            GetEligibleShipmentServicesResponse:
        """

        # GetEligibleShipmentServicesRequest
        data = {
            "ShipmentRequestDetails": shipment_request_details,
            **kwargs
        }

        return self._request(kwargs.pop('path'), data=data)

    @sp_endpoint("/mfn/v0/shipments/{}")
    def get_shipment(self, shipment_id: str, **kwargs) -> ApiResponse:
        """
        get_shipment(self, shipmentId:str) -> ApiResponse
        Returns a specified item and its attributes.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                MerchantFulfillment().get_shipment("abcddcba-00c3-4f6f-a63a-639f76ee9253")

        Args:
            shipment_id: str:

        Returns:
            GetShipmentResponse:
        """
        return self._request(fill_query_params(kwargs.pop('path'), shipment_id), params=kwargs, add_marketplace=False)

    @sp_endpoint("/mfn/v0/shipments/{}", method='DELETE')
    def cancel_shipment(self, shipment_id: str, **kwargs) -> ApiResponse:
        """
        cancel_shipment(self, shipment_id: str, **kwargs) -> ApiResponse
        Cancel the shipment indicated by the specified shipment identifier.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            shipment_id: str:

        Returns:
            CancelShipmentResponse:
        """
        return self._request(fill_query_params(kwargs.pop('path'), shipment_id), params=kwargs, add_marketplace=False)

    @sp_endpoint("/mfn/v0/shipments/{}/cancel", method='PUT')
    def cancel_shipment_old(self, shipment_id: str, **kwargs) -> ApiResponse:
        """
        cancel_shipment_old(self, shipment_id: str, **kwargs) -> ApiResponse
        Cancel the shipment indicated by the specified shipment identifer.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            shipment_id: str:

        Returns:
            CancelShipmentResponse:
        """
        return self._request(fill_query_params(kwargs.pop('path'), shipment_id), params=kwargs, add_marketplace=False)

    @sp_endpoint("/mfn/v0/shipments", method='POST')
    def create_shipment(self, shipment_request_details: dict, shipping_service_id: str, **kwargs) -> ApiResponse:
        """
        create_shipment(self, shipment_request_details: dict, shipping_service_id: str, **kwargs) -> ApiResponse
        Create a shipment with the information provided.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                MerchantFulfillment().create_shipment(
                        shipment_request_details={
                            "AmazonOrderId": "903-5563053-5647845",
                            "ItemList": [
                                {
                                    "OrderItemId": "52986411826454",
                                    "Quantity": 1
                                }
                            ],
                            "ShipFromAddress": {
                                "Name": "John Doe",
                                "AddressLine1": "300 Turnbull Ave",
                                "Email": "jdoeasdfllkj@yahoo.com",
                                "City": "Detroit",
                                "StateOrProvinceCode": "MI",
                                "PostalCode": "48123",
                                "CountryCode": "US",
                                "Phone": "7132341234"
                            },
                            "PackageDimensions": {
                                "Length": 10,
                                "Width": 10,
                                "Height": 10,
                                "Unit": "inches"
                            },
                            "Weight": {
                                "Value": 10,
                                "Unit": "oz"
                            },
                            "ShippingServiceOptions": {
                                "DeliveryExperience": "NoTracking",
                                "CarrierWillPickUp": False,
                                "CarrierWillPickUpOption": "ShipperWillDropOff"
                            }
                        },
                        shipping_service_id="UPS_PTP_2ND_DAY_AIR",
                        ShippingServiceOfferId="WHgxtyn6qjGGaCzOCog1azF5HLHje5Pz3Lc2Fmt5eKoZAReW8oJ1SMumuBS8lA/Hjuglhyiu0"
                                               "+KRLvyJxFV0PB9YFMDhygs3VyTL0WGYkGxiuRkmuEvpqldUn9rrkWVodqnR4vx2VtXvtER"
                                               "/Ju6RqYoddJZGy6RS2KLzzhQ2NclN0NYXMZVqpOe5RsRBddXaGuJr7oza3M52"
                                               "+JzChocAHzcurIhCRynpbxfmNLzZMQEbgnpGLzuaoSMzfxg90/NaXFR/Ou01du/uKd5AbfMW"
                                               "/AxAKP9ht6Oi9lDHq6WkGqvjkVLW0/jj/fBgblIwcs+t"
                    )

        Args:
            shipment_request_details: dict
            shipping_service_id: str:
        Returns:
            CreateShipmentResponse:
        """

        # CreateShipmentRequest
        data = {
            "ShipmentRequestDetails": shipment_request_details,
            "ShippingServiceId": shipping_service_id,
            **kwargs
        }
        return self._request(kwargs.pop('path'), data=data, add_marketplace=False)

    @sp_endpoint("/mfn/v0/sellerInputs", method='POST')
    def get_additional_seller_inputs_old(self, shipping_service_id: str,  ship_from_address: dict,
                                         order_id: str, **kwargs) -> ApiResponse:
        """
        get_additional_seller_inputs_old(self, shipping_service_id: str,  ship_from_address: dict, order_id: str,
        **kwargs) -> ApiResponse
        Get a list of additional seller inputs required for a ship method. This is generally
        used for international shipping.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            shipping_service_id: str
            ship_from_address: dict
            order_id: str:
        Returns:
            GetAdditionalSellerInputsResponse:
        """

        # GetAdditionalSellerInputsRequest
        data = {
            "ShippingServiceId": shipping_service_id,
            "ShipFromAddress": ship_from_address,
            "OrderId": order_id
        }
        return self._request(kwargs.pop('path'), data=data, add_marketplace=False)

    @sp_endpoint("/mfn/v0/additionalSellerInputs", method='POST')
    def get_additional_seller_inputs(self, shipping_service_id: str, ship_from_address: dict,
                                     order_id: str, **kwargs) -> ApiResponse:
        """
        get_additional_seller_inputs(self, shipping_service_id: str,  ship_from_address: dict, order_id: str,
        **kwargs) -> ApiResponse
        Gets a list of additional seller inputs required for a ship method. This is
        generally used for international shipping.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            shipping_service_id: str
            ship_from_address: dict
            order_id: str:
        Returns:
            GetAdditionalSellerInputsResponse:
        """

        # GetAdditionalSellerInputsRequest
        data = {
            "ShippingServiceId": shipping_service_id,
            "ShipFromAddress": ship_from_address,
            "OrderId": order_id
        }
        return self._request(kwargs.pop('path'), data=data, add_marketplace=False)
