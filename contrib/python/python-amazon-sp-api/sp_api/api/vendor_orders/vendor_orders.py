import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class VendorOrders(Client):
    """
    VendorOrders SP-API Client
    :link: 

    The Selling Partner API for Retail Procurement Orders provides programmatic access to vendor orders data.
    """


    @sp_endpoint('/vendor/orders/v1/purchaseOrders', method='GET')
    def get_purchase_orders(self, **kwargs) -> ApiResponse:
        """
        get_purchase_orders(self, **kwargs) -> ApiResponse

        Returns a list of purchase orders created or changed during the time frame that you specify. You define the time frame using the createdAfter, createdBefore, changedAfter and changedBefore parameters. The date range to search must not be more than 7 days. You can choose to get only the purchase order numbers by setting includeDetails to false. You can then use the getPurchaseOrder operation to receive details for a specific purchase order.

        **Usage Plans:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key limit:integer |  The limit to the number of records returned. Default value is 100 records.
            key createdAfter:string |  Purchase orders that became available after this time will be included in the result. Must be in ISO-8601 date/time format.
            key createdBefore:string |  Purchase orders that became available before this time will be included in the result. Must be in ISO-8601 date/time format.
            key sortOrder:string |  Sort in ascending or descending order by purchase order creation date.
            key nextToken:string |  Used for pagination when there is more purchase orders than the specified result size limit. The token value is returned in the previous API call
            key includeDetails:string |  When true, returns purchase orders with complete details. Otherwise, only purchase order numbers are returned. Default value is true.
            key changedAfter:string |  Purchase orders that changed after this timestamp will be included in the result. Must be in ISO-8601 date/time format.
            key changedBefore:string |  Purchase orders that changed before this timestamp will be included in the result. Must be in ISO-8601 date/time format.
            key poItemState:string |  Current state of the purchase order item. If this value is Cancelled, this API will return purchase orders which have one or more items cancelled by Amazon with updated item quantity as zero.
            key isPOChanged:string |  When true, returns purchase orders which were modified after the order was placed. Vendors are required to pull the changed purchase order and fulfill the updated purchase order and not the original one. Default value is false.
            key purchaseOrderState:string |  Filters purchase orders based on the purchase order state.
            key orderingVendorCode:string |  Filters purchase orders based on the specified ordering vendor code. This value should be same as 'sellingParty.partyId' in the purchase order. If not included in the filter, all purchase orders for all of the vendor codes that exist in the vendor group used to authorize the API client application are returned.

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  params=kwargs)
    

    @sp_endpoint('/vendor/orders/v1/purchaseOrders/{}', method='GET')
    def get_purchase_order(self, purchaseOrderNumber, **kwargs) -> ApiResponse:
        """
        get_purchase_order(self, purchaseOrderNumber, **kwargs) -> ApiResponse

        Returns a purchase order based on the purchaseOrderNumber value that you specify.

        **Usage Plans:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            purchaseOrderNumber:string | * REQUIRED The purchase order identifier for the order that you want. Formatting Notes: 8-character alpha-numeric code.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), purchaseOrderNumber), params=kwargs)
    

    @sp_endpoint('/vendor/orders/v1/acknowledgements', method='POST')
    def submit_acknowledgement(self, **kwargs) -> ApiResponse:
        """
        submit_acknowledgement(self, **kwargs) -> ApiResponse

        Submits acknowledgements for one or more purchase orders.

        **Usage Plans:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                {
                    "acknowledgements": [
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
                                    "countryCode": "st",
                                    "phone": "string"
                                },
                                "taxInfo": {
                                    "taxRegistrationType": "VAT",
                                    "taxRegistrationNumber": "string"
                                }
                            },
                            "acknowledgementDate": "2019-08-24T14:15:22Z",
                            "items": [
                                {
                                    "itemSequenceNumber": "string",
                                    "amazonProductIdentifier": "string",
                                    "vendorProductIdentifier": "string",
                                    "orderedQuantity": {
                                        "amount": 0,
                                        "unitOfMeasure": "Cases",
                                        "unitSize": 0
                                    },
                                    "netCost": {
                                        "currencyCode": "str",
                                        "amount": "string"
                                    },
                                    "listPrice": {
                                        "currencyCode": "str",
                                        "amount": "string"
                                    },
                                    "discountMultiplier": "string",
                                    "itemAcknowledgements": [
                                        {
                                            "acknowledgementCode": "Accepted",
                                            "acknowledgedQuantity": {
                                                "amount": 0,
                                                "unitOfMeasure": "Cases",
                                                "unitSize": 0
                                            },
                                            "scheduledShipDate": "2019-08-24T14:15:22Z",
                                            "scheduledDeliveryDate": "2019-08-24T14:15:22Z",
                                            "rejectionReason": "TemporarilyUnavailable"
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }


        Args:
            kwargs: acknowledgements | See Example

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  data=kwargs, add_marketplace=False)
    

    @sp_endpoint('/vendor/orders/v1/purchaseOrdersStatus', method='GET')
    def get_purchase_orders_status(self, **kwargs) -> ApiResponse:
        """
        get_purchase_orders_status(self, **kwargs) -> ApiResponse

        Returns purchase order statuses based on the filters that you specify. Date range to search must not be more than 7 days. You can return a list of purchase order statuses using the available filters, or a single purchase order status by providing the purchase order number.

        **Usage Plans:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key limit:integer |  The limit to the number of records returned. Default value is 100 records.
            key sortOrder:string |  Sort in ascending or descending order by purchase order creation date.
            key nextToken:string |  Used for pagination when there are more purchase orders than the specified result size limit.
            key createdAfter:string |  Purchase orders that became available after this timestamp will be included in the result. Must be in ISO-8601 date/time format.
            key createdBefore:string |  Purchase orders that became available before this timestamp will be included in the result. Must be in ISO-8601 date/time format.
            key updatedAfter:string |  Purchase orders for which the last purchase order update happened after this timestamp will be included in the result. Must be in ISO-8601 date/time format.
            key updatedBefore:string |  Purchase orders for which the last purchase order update happened before this timestamp will be included in the result. Must be in ISO-8601 date/time format.
            key purchaseOrderNumber:string |  Provides purchase order status for the specified purchase order number.
            key purchaseOrderStatus:string |  Filters purchase orders based on the specified purchase order status. If not included in filter, this will return purchase orders for all statuses.
            key itemConfirmationStatus:string |  Filters purchase orders based on the specified purchase order item status. If not included in filter, purchase orders for all statuses are included.
            key orderingVendorCode:string |  Filters purchase orders based on the specified ordering vendor code. This value should be same as 'sellingParty.partyId' in the purchase order. If not included in filter, all purchase orders for all the vendor codes that exist in the vendor group used to authorize API client application are returned.
            key shipToPartyId:string |  Filters purchase orders for a specific buyer's Fulfillment Center/warehouse by providing ship to location id here. This value should be same as 'shipToParty.partyId' in the purchase order. If not included in filter, this will return purchase orders for all the buyer's warehouses used for vendor group purchase orders.
        

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  params=kwargs)
    
