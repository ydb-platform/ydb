# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from typing import ClassVar, Dict, List, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.forwarding._request_create_params import (
        RequestCreateParams,
    )
    from stripe.params.forwarding._request_list_params import RequestListParams
    from stripe.params.forwarding._request_retrieve_params import (
        RequestRetrieveParams,
    )


class Request(
    CreateableAPIResource["Request"], ListableAPIResource["Request"]
):
    """
    Instructs Stripe to make a request on your behalf using the destination URL. The destination URL
    is activated by Stripe at the time of onboarding. Stripe verifies requests with your credentials
    provided during onboarding, and injects card details from the payment_method into the request.

    Stripe redacts all sensitive fields and headers, including authentication credentials and card numbers,
    before storing the request and response data in the forwarding Request object, which are subject to a
    30-day retention period.

    You can provide a Stripe idempotency key to make sure that requests with the same key result in only one
    outbound request. The Stripe idempotency key provided should be unique and different from any idempotency
    keys provided on the underlying third-party request.

    Forwarding Requests are synchronous requests that return a response or time out according to
    Stripe's limits.

    Related guide: [Forward card details to third-party API endpoints](https://docs.stripe.com/payments/forwarding).
    """

    OBJECT_NAME: ClassVar[Literal["forwarding.request"]] = "forwarding.request"

    class RequestContext(StripeObject):
        destination_duration: int
        """
        The time it took in milliseconds for the destination endpoint to respond.
        """
        destination_ip_address: str
        """
        The IP address of the destination.
        """

    class RequestDetails(StripeObject):
        class Header(StripeObject):
            name: str
            """
            The header name.
            """
            value: str
            """
            The header value.
            """

        body: str
        """
        The body payload to send to the destination endpoint.
        """
        headers: List[Header]
        """
        The headers to include in the forwarded request. Can be omitted if no additional headers (excluding Stripe-generated ones such as the Content-Type header) should be included.
        """
        http_method: Literal["POST"]
        """
        The HTTP method used to call the destination endpoint.
        """
        _inner_class_types = {"headers": Header}

    class ResponseDetails(StripeObject):
        class Header(StripeObject):
            name: str
            """
            The header name.
            """
            value: str
            """
            The header value.
            """

        body: str
        """
        The response body from the destination endpoint to Stripe.
        """
        headers: List[Header]
        """
        HTTP headers that the destination endpoint returned.
        """
        status: int
        """
        The HTTP status code that the destination endpoint returned.
        """
        _inner_class_types = {"headers": Header}

    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["forwarding.request"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    payment_method: str
    """
    The PaymentMethod to insert into the forwarded request. Forwarding previously consumed PaymentMethods is allowed.
    """
    replacements: List[
        Literal[
            "card_cvc",
            "card_expiry",
            "card_number",
            "cardholder_name",
            "request_signature",
        ]
    ]
    """
    The field kinds to be replaced in the forwarded request.
    """
    request_context: Optional[RequestContext]
    """
    Context about the request from Stripe's servers to the destination endpoint.
    """
    request_details: Optional[RequestDetails]
    """
    The request that was sent to the destination endpoint. We redact any sensitive fields.
    """
    response_details: Optional[ResponseDetails]
    """
    The response that the destination endpoint returned to us. We redact any sensitive fields.
    """
    url: Optional[str]
    """
    The destination URL for the forwarded request. Must be supported by the config.
    """

    @classmethod
    def create(cls, **params: Unpack["RequestCreateParams"]) -> "Request":
        """
        Creates a ForwardingRequest object.
        """
        return cast(
            "Request",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["RequestCreateParams"]
    ) -> "Request":
        """
        Creates a ForwardingRequest object.
        """
        return cast(
            "Request",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["RequestListParams"]
    ) -> ListObject["Request"]:
        """
        Lists all ForwardingRequest objects.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["RequestListParams"]
    ) -> ListObject["Request"]:
        """
        Lists all ForwardingRequest objects.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["RequestRetrieveParams"]
    ) -> "Request":
        """
        Retrieves a ForwardingRequest object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["RequestRetrieveParams"]
    ) -> "Request":
        """
        Retrieves a ForwardingRequest object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "request_context": RequestContext,
        "request_details": RequestDetails,
        "response_details": ResponseDetails,
    }
