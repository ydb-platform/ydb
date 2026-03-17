# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._balance_transaction import BalanceTransaction
    from stripe._source import Source
    from stripe.params._topup_cancel_params import TopupCancelParams
    from stripe.params._topup_create_params import TopupCreateParams
    from stripe.params._topup_list_params import TopupListParams
    from stripe.params._topup_modify_params import TopupModifyParams
    from stripe.params._topup_retrieve_params import TopupRetrieveParams


class Topup(
    CreateableAPIResource["Topup"],
    ListableAPIResource["Topup"],
    UpdateableAPIResource["Topup"],
):
    """
    To top up your Stripe balance, you create a top-up object. You can retrieve
    individual top-ups, as well as list all top-ups. Top-ups are identified by a
    unique, random ID.

    Related guide: [Topping up your platform account](https://docs.stripe.com/connect/top-ups)
    """

    OBJECT_NAME: ClassVar[Literal["topup"]] = "topup"
    amount: int
    """
    Amount transferred.
    """
    balance_transaction: Optional[ExpandableField["BalanceTransaction"]]
    """
    ID of the balance transaction that describes the impact of this top-up on your account balance. May not be specified depending on status of top-up.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    description: Optional[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    expected_availability_date: Optional[int]
    """
    Date the funds are expected to arrive in your Stripe account for payouts. This factors in delays like weekends or bank holidays. May not be specified depending on status of top-up.
    """
    failure_code: Optional[str]
    """
    Error code explaining reason for top-up failure if available (see [the errors section](https://docs.stripe.com/api#errors) for a list of codes).
    """
    failure_message: Optional[str]
    """
    Message to user further explaining reason for top-up failure if available.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["topup"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    source: Optional["Source"]
    """
    The source field is deprecated. It might not always be present in the API response.
    """
    statement_descriptor: Optional[str]
    """
    Extra information about a top-up. This will appear on your source's bank statement. It must contain at least one letter.
    """
    status: Literal["canceled", "failed", "pending", "reversed", "succeeded"]
    """
    The status of the top-up is either `canceled`, `failed`, `pending`, `reversed`, or `succeeded`.
    """
    transfer_group: Optional[str]
    """
    A string that identifies this top-up as part of a group.
    """

    @classmethod
    def _cls_cancel(
        cls, topup: str, **params: Unpack["TopupCancelParams"]
    ) -> "Topup":
        """
        Cancels a top-up. Only pending top-ups can be canceled.
        """
        return cast(
            "Topup",
            cls._static_request(
                "post",
                "/v1/topups/{topup}/cancel".format(topup=sanitize_id(topup)),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def cancel(topup: str, **params: Unpack["TopupCancelParams"]) -> "Topup":
        """
        Cancels a top-up. Only pending top-ups can be canceled.
        """
        ...

    @overload
    def cancel(self, **params: Unpack["TopupCancelParams"]) -> "Topup":
        """
        Cancels a top-up. Only pending top-ups can be canceled.
        """
        ...

    @class_method_variant("_cls_cancel")
    def cancel(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["TopupCancelParams"]
    ) -> "Topup":
        """
        Cancels a top-up. Only pending top-ups can be canceled.
        """
        return cast(
            "Topup",
            self._request(
                "post",
                "/v1/topups/{topup}/cancel".format(
                    topup=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_cancel_async(
        cls, topup: str, **params: Unpack["TopupCancelParams"]
    ) -> "Topup":
        """
        Cancels a top-up. Only pending top-ups can be canceled.
        """
        return cast(
            "Topup",
            await cls._static_request_async(
                "post",
                "/v1/topups/{topup}/cancel".format(topup=sanitize_id(topup)),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def cancel_async(
        topup: str, **params: Unpack["TopupCancelParams"]
    ) -> "Topup":
        """
        Cancels a top-up. Only pending top-ups can be canceled.
        """
        ...

    @overload
    async def cancel_async(
        self, **params: Unpack["TopupCancelParams"]
    ) -> "Topup":
        """
        Cancels a top-up. Only pending top-ups can be canceled.
        """
        ...

    @class_method_variant("_cls_cancel_async")
    async def cancel_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["TopupCancelParams"]
    ) -> "Topup":
        """
        Cancels a top-up. Only pending top-ups can be canceled.
        """
        return cast(
            "Topup",
            await self._request_async(
                "post",
                "/v1/topups/{topup}/cancel".format(
                    topup=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(cls, **params: Unpack["TopupCreateParams"]) -> "Topup":
        """
        Top up the balance of an account
        """
        return cast(
            "Topup",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["TopupCreateParams"]
    ) -> "Topup":
        """
        Top up the balance of an account
        """
        return cast(
            "Topup",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(cls, **params: Unpack["TopupListParams"]) -> ListObject["Topup"]:
        """
        Returns a list of top-ups.
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
        cls, **params: Unpack["TopupListParams"]
    ) -> ListObject["Topup"]:
        """
        Returns a list of top-ups.
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
    def modify(cls, id: str, **params: Unpack["TopupModifyParams"]) -> "Topup":
        """
        Updates the metadata of a top-up. Other top-up details are not editable by design.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Topup",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["TopupModifyParams"]
    ) -> "Topup":
        """
        Updates the metadata of a top-up. Other top-up details are not editable by design.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Topup",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["TopupRetrieveParams"]
    ) -> "Topup":
        """
        Retrieves the details of a top-up that has previously been created. Supply the unique top-up ID that was returned from your previous request, and Stripe will return the corresponding top-up information.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["TopupRetrieveParams"]
    ) -> "Topup":
        """
        Retrieves the details of a top-up that has previously been created. Supply the unique top-up ID that was returned from your previous request, and Stripe will return the corresponding top-up information.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance
