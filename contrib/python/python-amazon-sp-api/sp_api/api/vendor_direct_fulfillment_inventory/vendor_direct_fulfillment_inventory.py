import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class VendorDirectFulfillmentInventory(Client):
    """
    VendorDirectFulfillmentInventory SP-API Client
    :link: 

    The Selling Partner API for Direct Fulfillment Inventory Updates provides programmatic access to a direct fulfillment vendor's inventory updates.
    """


    @sp_endpoint('/vendor/directFulfillment/inventory/v1/warehouses/{}/items', method='POST')
    def submit_inventory_update(self, warehouseId, **kwargs) -> ApiResponse:
        """
        submit_inventory_update(self, warehouseId, **kwargs) -> ApiResponse

        Submits inventory updates for the specified warehouse for either a partial or full feed of inventory items.

        **Usage Plans:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            warehouseId:string | * REQUIRED Identifier for the warehouse for which to update inventory.
            body: {
              "inventory": {
                "sellingParty": {
                  "partyId": "string"
                },
                "isFullUpdate": true,
                "items": [
                  {
                    "buyerProductIdentifier": "string",
                    "vendorProductIdentifier": "string",
                    "availableQuantity": {
                      "amount": 0,
                      "unitOfMeasure": "string"
                    },
                    "isObsolete": true
                  }
                ]
              }
            }

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), warehouseId), data=kwargs, add_marketplace=False)
    
