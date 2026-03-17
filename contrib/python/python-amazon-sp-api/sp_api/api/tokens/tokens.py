import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class Tokens(Client):
    """
    Tokens SP-API Client
    :link: 

    The Selling Partner API for Tokens provides a secure way to access a customers's PII (Personally Identifiable Information). You can call the Tokens API to get a Restricted Data Token (RDT) for one or more restricted resources that you specify. The RDT authorizes you to make subsequent requests to access these restricted resources.
    """


    @sp_endpoint('/tokens/2021-03-01/restrictedDataToken', method='POST')
    def create_restricted_data_token(self, **kwargs) -> ApiResponse:
        """
        create_restricted_data_token(self, **kwargs) -> ApiResponse

        Returns a Restricted Data Token (RDT) for one or more restricted resources that you specify. A restricted resource is the HTTP method and path from a restricted operation that returns Personally Identifiable Information (PII). See the Tokens API Use Case Guide for a list of restricted operations. Use the RDT returned here as the access token in subsequent calls to the corresponding restricted operations.

        The path of a restricted resource can be:
        - A specific path containing a seller's order ID, for example ```/orders/v0/orders/902-3159896-1390916/address```. The returned RDT authorizes a subsequent call to the getOrderAddress operation of the Orders API for that specific order only. For example, ```GET /orders/v0/orders/902-3159896-1390916/address```.
        - A generic path that does not contain a seller's order ID, for example```/orders/v0/orders/{orderId}/address```). The returned RDT authorizes subsequent calls to the getOrderAddress operation for *any* of a seller's order IDs. For example, ```GET /orders/v0/orders/902-3159896-1390916/address``` and ```GET /orders/v0/orders/483-3488972-0896720/address```

        **Usage Plans:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                Tokens().create_restricted_data_token(restrictedResources=[
                     {
                         "method": "GET",
                         "path": "/orders/v0/orders",
                         "dataElements": ["buyerInfo", "shippingAddress"]
                     }
                  ])

        Args:
            body: {
              "targetApplication": "string",
              "restrictedResources": [
                {
                  "method": "GET",
                  "path": "string",
                  "dataElements": [
                    "string"
                  ]
                }
              ]
            }

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  data=kwargs)
    
