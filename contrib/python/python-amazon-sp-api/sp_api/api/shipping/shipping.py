import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class Shipping(Client):
    """
    Shipping SP-API Client
    :link: 

    Provides programmatic access to Amazon Shipping APIs.
    """


    @sp_endpoint('/shipping/v1/shipments', method='POST')
    def create_shipment(self, **kwargs) -> ApiResponse:
        """
        create_shipment(self, **kwargs) -> ApiResponse

        Create a new shipment.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       15
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            body: {
              "clientReferenceId": "string",
              "shipTo": {
                "name": "string",
                "addressLine1": "string",
                "addressLine2": "string",
                "addressLine3": "string",
                "stateOrRegion": "string",
                "city": "string",
                "countryCode": "st",
                "postalCode": "string",
                "email": "string",
                "copyEmails": [
                  "string"
                ],
                "phoneNumber": "string"
              },
              "shipFrom": {
                "name": "string",
                "addressLine1": "string",
                "addressLine2": "string",
                "addressLine3": "string",
                "stateOrRegion": "string",
                "city": "string",
                "countryCode": "st",
                "postalCode": "string",
                "email": "string",
                "copyEmails": [
                  "string"
                ],
                "phoneNumber": "string"
              },
              "containers": [
                {
                  "containerType": "PACKAGE",
                  "containerReferenceId": "string",
                  "value": {
                    "value": 0,
                    "unit": "str"
                  },
                  "dimensions": {
                    "length": 0,
                    "width": 0,
                    "height": 0,
                    "unit": "IN"
                  },
                  "items": [
                    {
                      "quantity": 0,
                      "unitPrice": {
                        "value": 0,
                        "unit": "str"
                      },
                      "unitWeight": {
                        "unit": "g",
                        "value": 0
                      },
                      "title": "string"
                    }
                  ],
                  "weight": {
                    "unit": "g",
                    "value": 0
                  }
                }
              ]
            }

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  data=kwargs)

    @sp_endpoint('/shipping/v1/shipments/{}', method='GET')
    def get_shipment(self, shipmentId, **kwargs) -> ApiResponse:
        """
        get_shipment(self, shipmentId, **kwargs) -> ApiResponse

        Return the entire shipment object for the shipmentId.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       15
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            shipmentId:string | * REQUIRED

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), shipmentId), params=kwargs)

    @sp_endpoint('/shipping/v1/shipments/{}/cancel', method='POST')
    def cancel_shipment(self, shipmentId, **kwargs) -> ApiResponse:
        """
        cancel_shipment(self, shipmentId, **kwargs) -> ApiResponse

        Cancel a shipment by the given shipmentId.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       15
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            shipmentId:string | * REQUIRED

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), shipmentId), data=kwargs)
    

    @sp_endpoint('/shipping/v1/shipments/{}/purchaseLabels', method='POST')
    def purchase_labels(self, shipmentId, **kwargs) -> ApiResponse:
        """
        purchase_labels(self, shipmentId, **kwargs) -> ApiResponse

        Purchase shipping labels based on a given rate.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       15
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            shipmentId:string | * REQUIRED
            body: {
              "rateId": "string",
              "labelSpecification": {
                "labelFormat": "PNG",
                "labelStockSize": "4x6"
              }
            }
        

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), shipmentId), data=kwargs)
    

    @sp_endpoint('/shipping/v1/shipments/{}/label', method='POST')
    def retrieve_shipping_label(self, shipmentId, **kwargs) -> ApiResponse:
        """
        retrieve_shipping_label(self, shipmentId, **kwargs) -> ApiResponse

        Retrieve shipping label based on the shipment id and tracking id.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       15
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            shipmentId:string | * REQUIRED
            trackingId:string | * REQUIRED
            body: {
              "labelSpecification": {
                "labelFormat": "PNG",
                "labelStockSize": "4x6"
              }
            }

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), shipmentId), data=kwargs)
    

    @sp_endpoint('/shipping/v1/purchaseShipment', method='POST')
    def purchase_shipment(self, **kwargs) -> ApiResponse:
        """
        purchase_shipment(self, **kwargs) -> ApiResponse

        Purchase shipping labels.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       15
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            body: {
              "clientReferenceId": "string",
              "shipTo": {
                "name": "string",
                "addressLine1": "string",
                "addressLine2": "string",
                "addressLine3": "string",
                "stateOrRegion": "string",
                "city": "string",
                "countryCode": "st",
                "postalCode": "string",
                "email": "string",
                "copyEmails": [
                  "string"
                ],
                "phoneNumber": "string"
              },
              "shipFrom": {
                "name": "string",
                "addressLine1": "string",
                "addressLine2": "string",
                "addressLine3": "string",
                "stateOrRegion": "string",
                "city": "string",
                "countryCode": "st",
                "postalCode": "string",
                "email": "string",
                "copyEmails": [
                  "string"
                ],
                "phoneNumber": "string"
              },
              "shipDate": "2019-08-24T14:15:22Z",
              "serviceType": "Amazon Shipping Ground",
              "containers": [
                {
                  "containerType": "PACKAGE",
                  "containerReferenceId": "string",
                  "value": {
                    "value": 0,
                    "unit": "str"
                  },
                  "dimensions": {
                    "length": 0,
                    "width": 0,
                    "height": 0,
                    "unit": "IN"
                  },
                  "items": [
                    {
                      "quantity": 0,
                      "unitPrice": {
                        "value": 0,
                        "unit": "str"
                      },
                      "unitWeight": {
                        "unit": "g",
                        "value": 0
                      },
                      "title": "string"
                    }
                  ],
                  "weight": {
                    "unit": "g",
                    "value": 0
                  }
                }
              ],
              "labelSpecification": {
                "labelFormat": "PNG",
                "labelStockSize": "4x6"
              }
            }

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  data=kwargs)
    

    @sp_endpoint('/shipping/v1/rates', method='POST')
    def get_rates(self, **kwargs) -> ApiResponse:
        """
        get_rates(self, **kwargs) -> ApiResponse

        Get service rates.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       15
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            body:{
              "shipTo": {
                "name": "string",
                "addressLine1": "string",
                "addressLine2": "string",
                "addressLine3": "string",
                "stateOrRegion": "string",
                "city": "string",
                "countryCode": "st",
                "postalCode": "string",
                "email": "string",
                "copyEmails": [
                  "string"
                ],
                "phoneNumber": "string"
              },
              "shipFrom": {
                "name": "string",
                "addressLine1": "string",
                "addressLine2": "string",
                "addressLine3": "string",
                "stateOrRegion": "string",
                "city": "string",
                "countryCode": "st",
                "postalCode": "string",
                "email": "string",
                "copyEmails": [
                  "string"
                ],
                "phoneNumber": "string"
              },
              "serviceTypes": [
                "Amazon Shipping Ground"
              ],
              "shipDate": "2019-08-24T14:15:22Z",
              "containerSpecifications": [
                {
                  "dimensions": {
                    "length": 0,
                    "width": 0,
                    "height": 0,
                    "unit": "IN"
                  },
                  "weight": {
                    "unit": "g",
                    "value": 0
                  }
                }
              ]
            }

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  data=kwargs)
    

    @sp_endpoint('/shipping/v1/account', method='GET')
    def get_account(self, **kwargs) -> ApiResponse:
        """
        get_account(self, **kwargs) -> ApiResponse

        Verify if the current account is valid.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       15
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  params=kwargs)
    

    @sp_endpoint('/shipping/v1/tracking/{}', method='GET')
    def get_tracking_information(self, trackingId, **kwargs) -> ApiResponse:
        """
        get_tracking_information(self, trackingId, **kwargs) -> ApiResponse

        Return the tracking information of a shipment.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            trackingId:string | * REQUIRED

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), trackingId), params=kwargs)
    
