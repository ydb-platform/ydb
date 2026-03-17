# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._charge import Charge
    from stripe._payment_intent import PaymentIntent
    from stripe.params._review_approve_params import ReviewApproveParams
    from stripe.params._review_list_params import ReviewListParams
    from stripe.params._review_retrieve_params import ReviewRetrieveParams


class Review(ListableAPIResource["Review"]):
    """
    Reviews can be used to supplement automated fraud detection with human expertise.

    Learn more about [Radar](https://docs.stripe.com/radar) and reviewing payments
    [here](https://docs.stripe.com/radar/reviews).
    """

    OBJECT_NAME: ClassVar[Literal["review"]] = "review"

    class IpAddressLocation(StripeObject):
        city: Optional[str]
        """
        The city where the payment originated.
        """
        country: Optional[str]
        """
        Two-letter ISO code representing the country where the payment originated.
        """
        latitude: Optional[float]
        """
        The geographic latitude where the payment originated.
        """
        longitude: Optional[float]
        """
        The geographic longitude where the payment originated.
        """
        region: Optional[str]
        """
        The state/county/province/region where the payment originated.
        """

    class Session(StripeObject):
        browser: Optional[str]
        """
        The browser used in this browser session (e.g., `Chrome`).
        """
        device: Optional[str]
        """
        Information about the device used for the browser session (e.g., `Samsung SM-G930T`).
        """
        platform: Optional[str]
        """
        The platform for the browser session (e.g., `Macintosh`).
        """
        version: Optional[str]
        """
        The version for the browser session (e.g., `61.0.3163.100`).
        """

    billing_zip: Optional[str]
    """
    The ZIP or postal code of the card used, if applicable.
    """
    charge: Optional[ExpandableField["Charge"]]
    """
    The charge associated with this review.
    """
    closed_reason: Optional[
        Literal[
            "acknowledged",
            "approved",
            "canceled",
            "disputed",
            "payment_never_settled",
            "redacted",
            "refunded",
            "refunded_as_fraud",
        ]
    ]
    """
    The reason the review was closed, or null if it has not yet been closed. One of `approved`, `refunded`, `refunded_as_fraud`, `disputed`, `redacted`, `canceled`, `payment_never_settled`, or `acknowledged`.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    id: str
    """
    Unique identifier for the object.
    """
    ip_address: Optional[str]
    """
    The IP address where the payment originated.
    """
    ip_address_location: Optional[IpAddressLocation]
    """
    Information related to the location of the payment. Note that this information is an approximation and attempts to locate the nearest population center - it should not be used to determine a specific address.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["review"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    open: bool
    """
    If `true`, the review needs action.
    """
    opened_reason: Literal["manual", "rule"]
    """
    The reason the review was opened. One of `rule` or `manual`.
    """
    payment_intent: Optional[ExpandableField["PaymentIntent"]]
    """
    The PaymentIntent ID associated with this review, if one exists.
    """
    reason: str
    """
    The reason the review is currently open or closed. One of `rule`, `manual`, `approved`, `refunded`, `refunded_as_fraud`, `disputed`, `redacted`, `canceled`, `payment_never_settled`, or `acknowledged`.
    """
    session: Optional[Session]
    """
    Information related to the browsing session of the user who initiated the payment.
    """

    @classmethod
    def _cls_approve(
        cls, review: str, **params: Unpack["ReviewApproveParams"]
    ) -> "Review":
        """
        Approves a Review object, closing it and removing it from the list of reviews.
        """
        return cast(
            "Review",
            cls._static_request(
                "post",
                "/v1/reviews/{review}/approve".format(
                    review=sanitize_id(review)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def approve(
        review: str, **params: Unpack["ReviewApproveParams"]
    ) -> "Review":
        """
        Approves a Review object, closing it and removing it from the list of reviews.
        """
        ...

    @overload
    def approve(self, **params: Unpack["ReviewApproveParams"]) -> "Review":
        """
        Approves a Review object, closing it and removing it from the list of reviews.
        """
        ...

    @class_method_variant("_cls_approve")
    def approve(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReviewApproveParams"]
    ) -> "Review":
        """
        Approves a Review object, closing it and removing it from the list of reviews.
        """
        return cast(
            "Review",
            self._request(
                "post",
                "/v1/reviews/{review}/approve".format(
                    review=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_approve_async(
        cls, review: str, **params: Unpack["ReviewApproveParams"]
    ) -> "Review":
        """
        Approves a Review object, closing it and removing it from the list of reviews.
        """
        return cast(
            "Review",
            await cls._static_request_async(
                "post",
                "/v1/reviews/{review}/approve".format(
                    review=sanitize_id(review)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def approve_async(
        review: str, **params: Unpack["ReviewApproveParams"]
    ) -> "Review":
        """
        Approves a Review object, closing it and removing it from the list of reviews.
        """
        ...

    @overload
    async def approve_async(
        self, **params: Unpack["ReviewApproveParams"]
    ) -> "Review":
        """
        Approves a Review object, closing it and removing it from the list of reviews.
        """
        ...

    @class_method_variant("_cls_approve_async")
    async def approve_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReviewApproveParams"]
    ) -> "Review":
        """
        Approves a Review object, closing it and removing it from the list of reviews.
        """
        return cast(
            "Review",
            await self._request_async(
                "post",
                "/v1/reviews/{review}/approve".format(
                    review=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["ReviewListParams"]
    ) -> ListObject["Review"]:
        """
        Returns a list of Review objects that have open set to true. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
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
        cls, **params: Unpack["ReviewListParams"]
    ) -> ListObject["Review"]:
        """
        Returns a list of Review objects that have open set to true. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
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
        cls, id: str, **params: Unpack["ReviewRetrieveParams"]
    ) -> "Review":
        """
        Retrieves a Review object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["ReviewRetrieveParams"]
    ) -> "Review":
        """
        Retrieves a Review object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "ip_address_location": IpAddressLocation,
        "session": Session,
    }
