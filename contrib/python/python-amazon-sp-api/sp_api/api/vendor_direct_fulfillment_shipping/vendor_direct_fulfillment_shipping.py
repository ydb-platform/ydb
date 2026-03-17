import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class VendorDirectFulfillmentShipping(Client):
    """
    VendorDirectFulfillmentShipping SP-API Client
    :link: 

    The Selling Partner API for Direct Fulfillment Shipping provides programmatic access to a direct fulfillment vendor's shipping data.
    """


    @sp_endpoint('/vendor/directFulfillment/shipping/v1/shippingLabels', method='GET')
    def get_shipping_labels(self, **kwargs) -> ApiResponse:
        """
        get_shipping_labels(self, **kwargs) -> ApiResponse

        Returns a list of shipping labels created during the time frame that you specify. You define that time frame using the createdAfter and createdBefore parameters. You must use both of these parameters. The date range to search must not be more than 7 days.

        **Usage Plans:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key shipFromPartyId:string |  The vendor warehouseId for order fulfillment. If not specified, the result will contain orders for all warehouses.
            key limit:integer |  The limit to the number of records returned.
            key createdAfter:string | * REQUIRED Shipping labels that became available after this date and time will be included in the result. Must be in ISO-8601 date/time format.
            key createdBefore:string | * REQUIRED Shipping labels that became available before this date and time will be included in the result. Must be in ISO-8601 date/time format.
            key sortOrder:string |  Sort ASC or DESC by order creation date.
            key nextToken:string |  Used for pagination when there are more ship labels than the specified result size limit. The token value is returned in the previous API call.

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  params=kwargs)
    

    @sp_endpoint('/vendor/directFulfillment/shipping/v1/shippingLabels', method='POST')
    def submit_shipping_label_request(self, **kwargs) -> ApiResponse:
        """
        submit_shipping_label_request(self, **kwargs) -> ApiResponse

        Creates a shipping label for a purchase order and returns a transactionId for reference.

        **Usage Plans:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            body: {
              "shippingLabelRequests": [
                {
                  "purchaseOrderNumber": "string",
                  "sellingParty": {
                    "partyId": "string",
                    "address": {
                      "name": "string",
                      "addressLine1": "string",
                      "addressLine2": "string",
                      "addressLine3": "string",
                      "city": "string",
                      "county": "string",
                      "district": "string",
                      "stateOrRegion": "string",
                      "postalCode": "string",
                      "countryCode": "string",
                      "phone": "string"
                    },
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string",
                        "taxRegistrationAddress": {
                          "name": "string",
                          "addressLine1": "string",
                          "addressLine2": "string",
                          "addressLine3": "string",
                          "city": "string",
                          "county": "string",
                          "district": "string",
                          "stateOrRegion": "string",
                          "postalCode": "string",
                          "countryCode": "string",
                          "phone": "string"
                        },
                        "taxRegistrationMessages": "string"
                      }
                    ]
                  },
                  "shipFromParty": {
                    "partyId": "string",
                    "address": {
                      "name": "string",
                      "addressLine1": "string",
                      "addressLine2": "string",
                      "addressLine3": "string",
                      "city": "string",
                      "county": "string",
                      "district": "string",
                      "stateOrRegion": "string",
                      "postalCode": "string",
                      "countryCode": "string",
                      "phone": "string"
                    },
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string",
                        "taxRegistrationAddress": {
                          "name": "string",
                          "addressLine1": "string",
                          "addressLine2": "string",
                          "addressLine3": "string",
                          "city": "string",
                          "county": "string",
                          "district": "string",
                          "stateOrRegion": "string",
                          "postalCode": "string",
                          "countryCode": "string",
                          "phone": "string"
                        },
                        "taxRegistrationMessages": "string"
                      }
                    ]
                  },
                  "containers": [
                    {
                      "containerType": "carton",
                      "containerIdentifier": "string",
                      "trackingNumber": "string",
                      "manifestId": "string",
                      "manifestDate": "string",
                      "shipMethod": "string",
                      "scacCode": "string",
                      "carrier": "string",
                      "containerSequenceNumber": 0,
                      "dimensions": {
                        "length": "string",
                        "width": "string",
                        "height": "string",
                        "unitOfMeasure": "IN"
                      },
                      "weight": {
                        "unitOfMeasure": "KG",
                        "value": "string"
                      },
                      "packedItems": [
                        {
                          "itemSequenceNumber": 0,
                          "buyerProductIdentifier": "string",
                          "vendorProductIdentifier": "string",
                          "packedQuantity": {
                            "amount": 0,
                            "unitOfMeasure": "string"
                          }
                        }
                      ]
                    }
                  ]
                }
              ]
            }

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  data=kwargs, add_marketplace=False)
    

    @sp_endpoint('/vendor/directFulfillment/shipping/v1/shippingLabels/{}', method='GET')
    def get_shipping_label(self, purchaseOrderNumber, **kwargs) -> ApiResponse:
        """
        get_shipping_label(self, purchaseOrderNumber, **kwargs) -> ApiResponse

        Returns a shipping label for the purchaseOrderNumber that you specify.

        **Usage Plans:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            purchaseOrderNumber:string | * REQUIRED The purchase order number for which you want to return the shipping label. It should be the same purchaseOrderNumber as received in the order.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), purchaseOrderNumber), params=kwargs)
    

    @sp_endpoint('/vendor/directFulfillment/shipping/v1/shipmentConfirmations', method='POST')
    def submit_shipment_confirmations(self, **kwargs) -> ApiResponse:
        """
        submit_shipment_confirmations(self, **kwargs) -> ApiResponse

        Submits one or more shipment confirmations for vendor orders.

        **Usage Plans:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            body: {
              "shipmentConfirmations": [
                {
                  "purchaseOrderNumber": "string",
                  "shipmentDetails": {
                    "shippedDate": "2019-08-24T14:15:22Z",
                    "shipmentStatus": "SHIPPED",
                    "isPriorityShipment": true,
                    "vendorOrderNumber": "string",
                    "estimatedDeliveryDate": "2019-08-24T14:15:22Z"
                  },
                  "sellingParty": {
                    "partyId": "string",
                    "address": {
                      "name": "string",
                      "addressLine1": "string",
                      "addressLine2": "string",
                      "addressLine3": "string",
                      "city": "string",
                      "county": "string",
                      "district": "string",
                      "stateOrRegion": "string",
                      "postalCode": "string",
                      "countryCode": "string",
                      "phone": "string"
                    },
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string",
                        "taxRegistrationAddress": {
                          "name": "string",
                          "addressLine1": "string",
                          "addressLine2": "string",
                          "addressLine3": "string",
                          "city": "string",
                          "county": "string",
                          "district": "string",
                          "stateOrRegion": "string",
                          "postalCode": "string",
                          "countryCode": "string",
                          "phone": "string"
                        },
                        "taxRegistrationMessages": "string"
                      }
                    ]
                  },
                  "shipFromParty": {
                    "partyId": "string",
                    "address": {
                      "name": "string",
                      "addressLine1": "string",
                      "addressLine2": "string",
                      "addressLine3": "string",
                      "city": "string",
                      "county": "string",
                      "district": "string",
                      "stateOrRegion": "string",
                      "postalCode": "string",
                      "countryCode": "string",
                      "phone": "string"
                    },
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string",
                        "taxRegistrationAddress": {
                          "name": "string",
                          "addressLine1": "string",
                          "addressLine2": "string",
                          "addressLine3": "string",
                          "city": "string",
                          "county": "string",
                          "district": "string",
                          "stateOrRegion": "string",
                          "postalCode": "string",
                          "countryCode": "string",
                          "phone": "string"
                        },
                        "taxRegistrationMessages": "string"
                      }
                    ]
                  },
                  "items": [
                    {
                      "itemSequenceNumber": 0,
                      "buyerProductIdentifier": "string",
                      "vendorProductIdentifier": "string",
                      "shippedQuantity": {
                        "amount": 0,
                        "unitOfMeasure": "string"
                      }
                    }
                  ],
                  "containers": [
                    {
                      "containerType": "carton",
                      "containerIdentifier": "string",
                      "trackingNumber": "string",
                      "manifestId": "string",
                      "manifestDate": "string",
                      "shipMethod": "string",
                      "scacCode": "string",
                      "carrier": "string",
                      "containerSequenceNumber": 0,
                      "dimensions": {
                        "length": "string",
                        "width": "string",
                        "height": "string",
                        "unitOfMeasure": "IN"
                      },
                      "weight": {
                        "unitOfMeasure": "KG",
                        "value": "string"
                      },
                      "packedItems": [
                        {
                          "itemSequenceNumber": 0,
                          "buyerProductIdentifier": "string",
                          "vendorProductIdentifier": "string",
                          "packedQuantity": {
                            "amount": 0,
                            "unitOfMeasure": "string"
                          }
                        }
                      ]
                    }
                  ]
                }
              ]
            }

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  data=kwargs, add_marketplace=False)
    

    @sp_endpoint('/vendor/directFulfillment/shipping/v1/shipmentStatusUpdates', method='POST')
    def submit_shipment_status_updates(self, **kwargs) -> ApiResponse:
        """
        submit_shipment_status_updates(self, **kwargs) -> ApiResponse

        This API call is only to be used by Vendor-Own-Carrier (VOC) vendors. Calling this API will submit a shipment status update for the package that a vendor has shipped. It will provide the Amazon customer visibility on their order, when the package is outside of Amazon Network visibility.

        **Usage Plans:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            body: {
              "shipmentStatusUpdates": [
                {
                  "purchaseOrderNumber": "string",
                  "sellingParty": {
                    "partyId": "string",
                    "address": {
                      "name": "string",
                      "addressLine1": "string",
                      "addressLine2": "string",
                      "addressLine3": "string",
                      "city": "string",
                      "county": "string",
                      "district": "string",
                      "stateOrRegion": "string",
                      "postalCode": "string",
                      "countryCode": "string",
                      "phone": "string"
                    },
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string",
                        "taxRegistrationAddress": {
                          "name": "string",
                          "addressLine1": "string",
                          "addressLine2": "string",
                          "addressLine3": "string",
                          "city": "string",
                          "county": "string",
                          "district": "string",
                          "stateOrRegion": "string",
                          "postalCode": "string",
                          "countryCode": "string",
                          "phone": "string"
                        },
                        "taxRegistrationMessages": "string"
                      }
                    ]
                  },
                  "shipFromParty": {
                    "partyId": "string",
                    "address": {
                      "name": "string",
                      "addressLine1": "string",
                      "addressLine2": "string",
                      "addressLine3": "string",
                      "city": "string",
                      "county": "string",
                      "district": "string",
                      "stateOrRegion": "string",
                      "postalCode": "string",
                      "countryCode": "string",
                      "phone": "string"
                    },
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string",
                        "taxRegistrationAddress": {
                          "name": "string",
                          "addressLine1": "string",
                          "addressLine2": "string",
                          "addressLine3": "string",
                          "city": "string",
                          "county": "string",
                          "district": "string",
                          "stateOrRegion": "string",
                          "postalCode": "string",
                          "countryCode": "string",
                          "phone": "string"
                        },
                        "taxRegistrationMessages": "string"
                      }
                    ]
                  },
                  "statusUpdateDetails": {
                    "trackingNumber": "string",
                    "statusCode": "string",
                    "reasonCode": "string",
                    "statusDateTime": "2019-08-24T14:15:22Z",
                    "statusLocationAddress": {
                      "name": "string",
                      "addressLine1": "string",
                      "addressLine2": "string",
                      "addressLine3": "string",
                      "city": "string",
                      "county": "string",
                      "district": "string",
                      "stateOrRegion": "string",
                      "postalCode": "string",
                      "countryCode": "string",
                      "phone": "string"
                    },
                    "shipmentSchedule": {
                      "estimatedDeliveryDateTime": "2019-08-24T14:15:22Z",
                      "apptWindowStartDateTime": "2019-08-24T14:15:22Z",
                      "apptWindowEndDateTime": "2019-08-24T14:15:22Z"
                    }
                  }
                }
              ]
            }

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  data=kwargs)
    

    @sp_endpoint('/vendor/directFulfillment/shipping/v1/customerInvoices', method='GET')
    def get_customer_invoices(self, **kwargs) -> ApiResponse:
        """
        get_customer_invoices(self, **kwargs) -> ApiResponse

        Returns a list of customer invoices created during a time frame that you specify. You define the  time frame using the createdAfter and createdBefore parameters. You must use both of these parameters. The date range to search must be no more than 7 days.

        **Usage Plans:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key shipFromPartyId:string |  The vendor warehouseId for order fulfillment. If not specified, the result will contain orders for all warehouses.
            key limit:integer |  The limit to the number of records returned
            key createdAfter:string | * REQUIRED Orders that became available after this date and time will be included in the result. Must be in ISO-8601 date/time format.
            key createdBefore:string | * REQUIRED Orders that became available before this date and time will be included in the result. Must be in ISO-8601 date/time format.
            key sortOrder:string |  Sort ASC or DESC by order creation date.
            key nextToken:string |  Used for pagination when there are more orders than the specified result size limit. The token value is returned in the previous API call.

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  params=kwargs)

    @sp_endpoint('/vendor/directFulfillment/shipping/v1/customerInvoices/{}', method='GET')
    def get_customer_invoice(self, purchaseOrderNumber, **kwargs) -> ApiResponse:
        """
        get_customer_invoice(self, purchaseOrderNumber, **kwargs) -> ApiResponse

        Returns a customer invoice based on the purchaseOrderNumber that you specify.

        **Usage Plans:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            purchaseOrderNumber:string | * REQUIRED Purchase order number of the shipment for which to return the invoice.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), purchaseOrderNumber), params=kwargs)
    

    @sp_endpoint('/vendor/directFulfillment/shipping/v1/packingSlips', method='GET')
    def get_packing_slips(self, **kwargs) -> ApiResponse:
        """
        get_packing_slips(self, **kwargs) -> ApiResponse

        Returns a list of packing slips for the purchase orders that match the criteria specified. Date range to search must not be more than 7 days.

        **Usage Plans:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key shipFromPartyId:string |  The vendor warehouseId for order fulfillment. If not specified the result will contain orders for all warehouses.
            key limit:integer |  The limit to the number of records returned
            key createdAfter:string | * REQUIRED Packing slips that became available after this date and time will be included in the result. Must be in ISO-8601 date/time format.
            key createdBefore:string | * REQUIRED Packing slips that became available before this date and time will be included in the result. Must be in ISO-8601 date/time format.
            key sortOrder:string |  Sort ASC or DESC by packing slip creation date.
            key nextToken:string |  Used for pagination when there are more packing slips than the specified result size limit. The token value is returned in the previous API call.

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  params=kwargs)
    

    @sp_endpoint('/vendor/directFulfillment/shipping/v1/packingSlips/{}', method='GET')
    def get_packing_slip(self, purchaseOrderNumber, **kwargs) -> ApiResponse:
        """
        get_packing_slip(self, purchaseOrderNumber, **kwargs) -> ApiResponse

        Returns a packing slip based on the purchaseOrderNumber that you specify.

        **Usage Plans:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            purchaseOrderNumber:string | * REQUIRED The purchaseOrderNumber for the packing slip you want.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), purchaseOrderNumber), params=kwargs)
    
