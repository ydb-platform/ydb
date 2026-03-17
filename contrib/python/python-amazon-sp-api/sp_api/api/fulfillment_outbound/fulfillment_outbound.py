import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class FulfillmentOutbound(Client):
    """
    FulfillmentOutbound SP-API Client
    :link: 

    The Selling Partner API for Fulfillment Outbound lets you create applications that help a seller fulfill Multi-Channel Fulfillment orders using their inventory in Amazon's fulfillment network. You can get information on both potential and existing fulfillment orders.
    """


    @sp_endpoint('/fba/outbound/2020-07-01/fulfillmentOrders/preview', method='POST')
    def get_fulfillment_preview(self, **kwargs) -> ApiResponse:
        """
        get_fulfillment_preview(self, **kwargs) -> ApiResponse

        Returns a list of fulfillment order previews based on shipping criteria that you specify.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       30
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                {
                      "marketplaceId": "string",
                      "address": {
                        "name": "string",
                        "addressLine1": "string",
                        "addressLine2": "string",
                        "addressLine3": "string",
                        "city": "string",
                        "districtOrCounty": "string",
                        "stateOrRegion": "string",
                        "postalCode": "string",
                        "countryCode": "string",
                        "phone": "string"
                      },
                      "items": [
                        {
                          "sellerSku": "string",
                          "quantity": 0,
                          "perUnitDeclaredValue": {
                            "currencyCode": "string",
                            "value": "string"
                          },
                          "sellerFulfillmentOrderItemId": "string"
                        }
                      ],
                      "shippingSpeedCategories": [
                        "Standard"
                      ],
                      "includeCODFulfillmentPreview": true,
                      "includeDeliveryWindows": true,
                      "featureConstraints": [
                        {
                          "featureName": "string",
                          "featureFulfillmentPolicy": "Required"
                        }
                      ]
                    }

        Args:
            body: {
              "marketplaceId": "string",
              "address": {
                "name": "string",
                "addressLine1": "string",
                "addressLine2": "string",
                "addressLine3": "string",
                "city": "string",
                "districtOrCounty": "string",
                "stateOrRegion": "string",
                "postalCode": "string",
                "countryCode": "string",
                "phone": "string"
              },
              "items": [
                {
                  "sellerSku": "string",
                  "quantity": 0,
                  "perUnitDeclaredValue": {
                    "currencyCode": "string",
                    "value": "string"
                  },
                  "sellerFulfillmentOrderItemId": "string"
                }
              ],
              "shippingSpeedCategories": [
                "Standard"
              ],
              "includeCODFulfillmentPreview": true,
              "includeDeliveryWindows": true,
              "featureConstraints": [
                {
                  "featureName": "string",
                  "featureFulfillmentPolicy": "Required"
                }
              ]
            }
        

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  data=kwargs)
    

    @sp_endpoint('/fba/outbound/2020-07-01/fulfillmentOrders', method='GET')
    def list_all_fulfillment_orders(self, **kwargs) -> ApiResponse:
        """
        list_all_fulfillment_orders(self, **kwargs) -> ApiResponse

        Returns a list of fulfillment orders fulfilled after (or at) a specified date-time, or indicated by the next token parameter.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       30
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key queryStartDate:string |  A date used to select fulfillment orders that were last updated after (or at) a specified time. An update is defined as any change in fulfillment order status, including the creation of a new fulfillment order.
            key nextToken:string |  A string token returned in the response to your previous request.

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  params=kwargs)
    

    @sp_endpoint('/fba/outbound/2020-07-01/fulfillmentOrders', method='POST')
    def create_fulfillment_order(self, **kwargs) -> ApiResponse:
        """
        create_fulfillment_order(self, **kwargs) -> ApiResponse

        Requests that Amazon ship items from the seller's inventory in Amazon's fulfillment network to a destination address.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       30
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                {
                      "marketplaceId": "string",
                      "sellerFulfillmentOrderId": "string",
                      "displayableOrderId": "string",
                      "displayableOrderDate": "2019-08-24T14:15:22Z",
                      "displayableOrderComment": "string",
                      "shippingSpeedCategory": "Standard",
                      "deliveryWindow": {
                        "startDate": "2019-08-24T14:15:22Z",
                        "endDate": "2019-08-24T14:15:22Z"
                      },
                      "destinationAddress": {
                        "name": "string",
                        "addressLine1": "string",
                        "addressLine2": "string",
                        "addressLine3": "string",
                        "city": "string",
                        "districtOrCounty": "string",
                        "stateOrRegion": "string",
                        "postalCode": "string",
                        "countryCode": "string",
                        "phone": "string"
                      },
                      "fulfillmentAction": "Ship",
                      "fulfillmentPolicy": "FillOrKill",
                      "codSettings": {
                        "isCodRequired": true,
                        "codCharge": {
                          "currencyCode": "string",
                          "value": "string"
                        },
                        "codChargeTax": {
                          "currencyCode": "string",
                          "value": "string"
                        },
                        "shippingCharge": {
                          "currencyCode": "string",
                          "value": "string"
                        },
                        "shippingChargeTax": {
                          "currencyCode": "string",
                          "value": "string"
                        }
                      },
                      "shipFromCountryCode": "string",
                      "notificationEmails": [
                        "string"
                      ],
                      "featureConstraints": [
                        {
                          "featureName": "string",
                          "featureFulfillmentPolicy": "Required"
                        }
                      ],
                      "items": [
                        {
                          "sellerSku": "string",
                          "sellerFulfillmentOrderItemId": "string",
                          "quantity": 0,
                          "giftMessage": "string",
                          "displayableComment": "string",
                          "fulfillmentNetworkSku": "string",
                          "perUnitDeclaredValue": {
                            "currencyCode": "string",
                            "value": "string"
                          },
                          "perUnitPrice": {
                            "currencyCode": "string",
                            "value": "string"
                          },
                          "perUnitTax": {
                            "currencyCode": "string",
                            "value": "string"
                          }
                        }
                      ]
                    }

        Args:
            body: {
              "marketplaceId": "string",
              "sellerFulfillmentOrderId": "string",
              "displayableOrderId": "string",
              "displayableOrderDate": "2019-08-24T14:15:22Z",
              "displayableOrderComment": "string",
              "shippingSpeedCategory": "Standard",
              "deliveryWindow": {
                "startDate": "2019-08-24T14:15:22Z",
                "endDate": "2019-08-24T14:15:22Z"
              },
              "destinationAddress": {
                "name": "string",
                "addressLine1": "string",
                "addressLine2": "string",
                "addressLine3": "string",
                "city": "string",
                "districtOrCounty": "string",
                "stateOrRegion": "string",
                "postalCode": "string",
                "countryCode": "string",
                "phone": "string"
              },
              "fulfillmentAction": "Ship",
              "fulfillmentPolicy": "FillOrKill",
              "codSettings": {
                "isCodRequired": true,
                "codCharge": {
                  "currencyCode": "string",
                  "value": "string"
                },
                "codChargeTax": {
                  "currencyCode": "string",
                  "value": "string"
                },
                "shippingCharge": {
                  "currencyCode": "string",
                  "value": "string"
                },
                "shippingChargeTax": {
                  "currencyCode": "string",
                  "value": "string"
                }
              },
              "shipFromCountryCode": "string",
              "notificationEmails": [
                "string"
              ],
              "featureConstraints": [
                {
                  "featureName": "string",
                  "featureFulfillmentPolicy": "Required"
                }
              ],
              "items": [
                {
                  "sellerSku": "string",
                  "sellerFulfillmentOrderItemId": "string",
                  "quantity": 0,
                  "giftMessage": "string",
                  "displayableComment": "string",
                  "fulfillmentNetworkSku": "string",
                  "perUnitDeclaredValue": {
                    "currencyCode": "string",
                    "value": "string"
                  },
                  "perUnitPrice": {
                    "currencyCode": "string",
                    "value": "string"
                  },
                  "perUnitTax": {
                    "currencyCode": "string",
                    "value": "string"
                  }
                }
              ]
            }
        

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  data=kwargs)
    

    @sp_endpoint('/fba/outbound/2020-07-01/tracking', method='GET')
    def get_package_tracking_details(self, **kwargs) -> ApiResponse:
        """
        get_package_tracking_details(self, **kwargs) -> ApiResponse

        Returns delivery tracking information for a package in an outbound shipment for a Multi-Channel Fulfillment order.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       30
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key packageNumber:integer | * REQUIRED The unencrypted package identifier returned by the getFulfillmentOrder operation.

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  params=kwargs)
    

    @sp_endpoint('/fba/outbound/2020-07-01/returnReasonCodes', method='GET')
    def list_return_reason_codes(self, **kwargs) -> ApiResponse:
        """
        list_return_reason_codes(self, **kwargs) -> ApiResponse

        Returns a list of return reason codes for a seller SKU in a given marketplace.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       30
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key sellerSku:string | * REQUIRED The seller SKU for which return reason codes are required.
            key marketplaceId:string |  The marketplace for which the seller wants return reason codes.
            key sellerFulfillmentOrderId:string |  The identifier assigned to the item by the seller when the fulfillment order was created. The service uses this value to determine the marketplace for which the seller wants return reason codes.
            key language:string | * REQUIRED The language that the TranslatedDescription property of the ReasonCodeDetails response object should be translated into.

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  params=kwargs)
    

    @sp_endpoint('/fba/outbound/2020-07-01/fulfillmentOrders/{}/return', method='PUT')
    def create_fulfillment_return(self, sellerFulfillmentOrderId, **kwargs) -> ApiResponse:
        """
        create_fulfillment_return(self, sellerFulfillmentOrderId, **kwargs) -> ApiResponse

        Creates a fulfillment return. 

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       30
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                {
                  "items": [
                    {
                      "sellerReturnItemId": "string",
                      "sellerFulfillmentOrderItemId": "string",
                      "amazonShipmentId": "string",
                      "returnReasonCode": "string",
                      "returnComment": "string"
                    }
                  ]
                }

        Args:
            sellerFulfillmentOrderId:string | * REQUIRED An identifier assigned by the seller to the fulfillment order at the time it was created. The seller uses their own records to find the correct SellerFulfillmentOrderId value based on the buyer's request to return items.
            body: {
              "items": [
                {
                  "sellerReturnItemId": "string",
                  "sellerFulfillmentOrderItemId": "string",
                  "amazonShipmentId": "string",
                  "returnReasonCode": "string",
                  "returnComment": "string"
                }
              ]
            }


         Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), sellerFulfillmentOrderId), data=kwargs)
    

    @sp_endpoint('/fba/outbound/2020-07-01/fulfillmentOrders/{}', method='GET')
    def get_fulfillment_order(self, sellerFulfillmentOrderId, **kwargs) -> ApiResponse:
        """
        get_fulfillment_order(self, sellerFulfillmentOrderId, **kwargs) -> ApiResponse

        Returns the fulfillment order indicated by the specified order identifier.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       30
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            sellerFulfillmentOrderId:string | * REQUIRED The identifier assigned to the item by the seller when the fulfillment order was created.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), sellerFulfillmentOrderId), params=kwargs)
    

    @sp_endpoint('/fba/outbound/2020-07-01/fulfillmentOrders/{}', method='PUT')
    def update_fulfillment_order(self, sellerFulfillmentOrderId, **kwargs) -> ApiResponse:
        """
        update_fulfillment_order(self, sellerFulfillmentOrderId, **kwargs) -> ApiResponse

        Updates and/or requests shipment for a fulfillment order with an order hold on it.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       30
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                {
                  "marketplaceId": "string",
                  "displayableOrderId": "string",
                  "displayableOrderDate": "2019-08-24T14:15:22Z",
                  "displayableOrderComment": "string",
                  "shippingSpeedCategory": "Standard",
                  "destinationAddress": {
                    "name": "string",
                    "addressLine1": "string",
                    "addressLine2": "string",
                    "addressLine3": "string",
                    "city": "string",
                    "districtOrCounty": "string",
                    "stateOrRegion": "string",
                    "postalCode": "string",
                    "countryCode": "string",
                    "phone": "string"
                  },
                  "fulfillmentAction": "Ship",
                  "fulfillmentPolicy": "FillOrKill",
                  "shipFromCountryCode": "string",
                  "notificationEmails": [
                    "string"
                  ],
                  "featureConstraints": [
                    {
                      "featureName": "string",
                      "featureFulfillmentPolicy": "Required"
                    }
                  ],
                  "items": [
                    {
                      "sellerSku": "string",
                      "sellerFulfillmentOrderItemId": "string",
                      "quantity": 0,
                      "giftMessage": "string",
                      "displayableComment": "string",
                      "fulfillmentNetworkSku": "string",
                      "orderItemDisposition": "string",
                      "perUnitDeclaredValue": {
                        "currencyCode": "string",
                        "value": "string"
                      },
                      "perUnitPrice": {
                        "currencyCode": "string",
                        "value": "string"
                      },
                      "perUnitTax": {
                        "currencyCode": "string",
                        "value": "string"
                      }
                    }
                  ]
                }

        Args:
            sellerFulfillmentOrderId:string | * REQUIRED The identifier assigned to the item by the seller when the fulfillment order was created.
            body: {
              "marketplaceId": "string",
              "displayableOrderId": "string",
              "displayableOrderDate": "2019-08-24T14:15:22Z",
              "displayableOrderComment": "string",
              "shippingSpeedCategory": "Standard",
              "destinationAddress": {
                "name": "string",
                "addressLine1": "string",
                "addressLine2": "string",
                "addressLine3": "string",
                "city": "string",
                "districtOrCounty": "string",
                "stateOrRegion": "string",
                "postalCode": "string",
                "countryCode": "string",
                "phone": "string"
              },
              "fulfillmentAction": "Ship",
              "fulfillmentPolicy": "FillOrKill",
              "shipFromCountryCode": "string",
              "notificationEmails": [
                "string"
              ],
              "featureConstraints": [
                {
                  "featureName": "string",
                  "featureFulfillmentPolicy": "Required"
                }
              ],
              "items": [
                {
                  "sellerSku": "string",
                  "sellerFulfillmentOrderItemId": "string",
                  "quantity": 0,
                  "giftMessage": "string",
                  "displayableComment": "string",
                  "fulfillmentNetworkSku": "string",
                  "orderItemDisposition": "string",
                  "perUnitDeclaredValue": {
                    "currencyCode": "string",
                    "value": "string"
                  },
                  "perUnitPrice": {
                    "currencyCode": "string",
                    "value": "string"
                  },
                  "perUnitTax": {
                    "currencyCode": "string",
                    "value": "string"
                  }
                }
              ]
            }



        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), sellerFulfillmentOrderId), data=kwargs)
    

    @sp_endpoint('/fba/outbound/2020-07-01/fulfillmentOrders/{}/cancel', method='PUT')
    def cancel_fulfillment_order(self, sellerFulfillmentOrderId, **kwargs) -> ApiResponse:
        """
        cancel_fulfillment_order(self, sellerFulfillmentOrderId, **kwargs) -> ApiResponse

        Requests that Amazon stop attempting to fulfill the fulfillment order indicated by the specified order identifier.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       30
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            sellerFulfillmentOrderId:string | * REQUIRED The identifier assigned to the item by the seller when the fulfillment order was created.
        

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), sellerFulfillmentOrderId), data=kwargs)
    

    @sp_endpoint('/fba/outbound/2020-07-01/features', method='GET')
    def get_features(self, **kwargs) -> ApiResponse:
        """
        get_features(self, **kwargs) -> ApiResponse

        Returns a list of features available for Multi-Channel Fulfillment orders in the marketplace you specify, and whether the seller for which you made the call is enrolled for each feature.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       30
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key marketplaceId:string | * REQUIRED The marketplace for which to return the list of features.

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  params=kwargs)
    

    @sp_endpoint('/fba/outbound/2020-07-01/features/inventory/{}', method='GET')
    def get_feature_inventory(self, featureName, **kwargs) -> ApiResponse:
        """
        get_feature_inventory(self, featureName, **kwargs) -> ApiResponse

        Returns a list of inventory items that are eligible for the fulfillment feature you specify.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       30
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key marketplaceId:string | * REQUIRED The marketplace for which to return a list of the inventory that is eligible for the specified feature.
            featureName:string | * REQUIRED The name of the feature for which to return a list of eligible inventory.
            key nextToken:string |  A string token returned in the response to your previous request that is used to return the next response page. A value of null will return the first page.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), featureName), params=kwargs)
    

    @sp_endpoint('/fba/outbound/2020-07-01/features/inventory/{}', method='GET')
    def get_feature_s_k_u(self, featureName, **kwargs) -> ApiResponse:
        """
        get_feature_s_k_u(self, featureName, **kwargs) -> ApiResponse

        Returns the number of items with the sellerSKU you specify that can have orders fulfilled using the specified feature. Note that if the sellerSKU isn't eligible, the response will contain an empty skuInfo object.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       30
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key marketplaceId:string | * REQUIRED The marketplace for which to return the count.
            featureName:string | * REQUIRED The name of the feature.
            sellerSku:string | * REQUIRED Used to identify an item in the given marketplace. SellerSKU is qualified by the seller's SellerId, which is included with every operation that you submit.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), featureName), params=kwargs)
    
