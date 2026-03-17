import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class VendorDirectFulfillmentTransactions(Client):
    """
    VendorDirectFulfillmentTransactions SP-API Client
    :link: 

    The Selling Partner API for Direct Fulfillment Transaction Status provides programmatic access to a direct fulfillment vendor's transaction status.
    """


    @sp_endpoint('/vendor/directFulfillment/transactions/v1/transactions/{}', method='GET')
    def get_transaction_status(self, transactionId, **kwargs) -> ApiResponse:
        """
        get_transaction_status(self, transactionId, **kwargs) -> ApiResponse

        Returns the status of the transaction indicated by the specified transactionId.

        **Usage Plans:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            transactionId:string | * REQUIRED Previously returned in the response to the POST request of a specific transaction.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), transactionId), params=kwargs)
    
