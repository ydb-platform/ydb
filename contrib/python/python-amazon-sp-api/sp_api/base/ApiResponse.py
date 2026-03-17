import pprint


class ApiResponse:
    """
    Api Response

    Wrapper around all responses from the API.

    Examples:
        literal blocks::

            response = Orders().get_orders(CreatedAfter='TEST_CASE_200', MarketplaceIds=["ATVPDKIKX0DER"])

            print(response.payload) # original response data
            # Access one of `payload`s properties using `__getattr__`
            print(response.Orders) # Array of orders
            # Access one of `payload`s properties using `__call__`
            print(response('Orders')) # Array of orders
            # Shorthand for response.payload
            print(response()) # original response data

    Args:
        payload: dict or list | original response from Amazon
        errors: any | contains possible error messages
        pagination: any | information about an endpoints pagination
        headers: any | headers returned by the API
        rate_limit: number | The `x-amzn-RateLimit-Limit` header, if available
        next_token: str | The next token used to retrieve the next page, if any
        kwargs: any

    """

    def __init__(
        self,
        payload=None,
        errors=None,
        pagination=None,
        headers=None,
        nextToken=None,
        **kwargs
    ):
        self.payload = payload or kwargs
        self.errors = errors
        self.pagination = pagination
        self.headers = headers
        self.rate_limit = headers.get("x-amzn-RateLimit-Limit")
        try:
            self.next_token = (
                nextToken
                or self.payload.get('pagination', {}).get("nextToken", None)
                or self.payload.get("NextToken", None)
                or self.pagination.get("nextToken", None)
            )

        except AttributeError:
            self.next_token = None
        if kwargs != self.payload:
            self.kwargs = kwargs

    def __str__(self):
        return pprint.pformat(vars(self))

    def __call__(self, item=None, **kwargs):
        if not item:
            return self.payload
        return self.payload.get(item)

    def __getattr__(self, item):
        return self.payload.get(item)
