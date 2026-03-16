import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class VendorTransactionStatus(Client):
    """
    VendorTransactionStatus SP-API Client
    :link: 

    The Selling Partner API for Retail Procurement Transaction Status provides programmatic access to status information on specific asynchronous POST transactions for vendors.
    """


    @sp_endpoint('/vendor/transactions/v1/transactions/{}', method='GET')
    def get_transaction(self, transactionId, **kwargs) -> ApiResponse:
        """
        get_transaction(self, transactionId, **kwargs) -> ApiResponse

        Returns the status of the transaction that you specify.

        **Usage Plans:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            transactionId:string | * REQUIRED The GUID provided by Amazon in the 'transactionId' field in response to the post request of a specific transaction.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), transactionId), params=kwargs)
    
