# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from typing import ClassVar, Dict, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.treasury._credit_reversal_create_params import (
        CreditReversalCreateParams,
    )
    from stripe.params.treasury._credit_reversal_list_params import (
        CreditReversalListParams,
    )
    from stripe.params.treasury._credit_reversal_retrieve_params import (
        CreditReversalRetrieveParams,
    )
    from stripe.treasury._transaction import Transaction


class CreditReversal(
    CreateableAPIResource["CreditReversal"],
    ListableAPIResource["CreditReversal"],
):
    """
    You can reverse some [ReceivedCredits](https://api.stripe.com#received_credits) depending on their network and source flow. Reversing a ReceivedCredit leads to the creation of a new object known as a CreditReversal.
    """

    OBJECT_NAME: ClassVar[Literal["treasury.credit_reversal"]] = (
        "treasury.credit_reversal"
    )

    class StatusTransitions(StripeObject):
        posted_at: Optional[int]
        """
        Timestamp describing when the CreditReversal changed status to `posted`
        """

    amount: int
    """
    Amount (in cents) transferred.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    financial_account: str
    """
    The FinancialAccount to reverse funds from.
    """
    hosted_regulatory_receipt_url: Optional[str]
    """
    A [hosted transaction receipt](https://docs.stripe.com/treasury/moving-money/regulatory-receipts) URL that is provided when money movement is considered regulated under Stripe's money transmission licenses.
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
    network: Literal["ach", "stripe"]
    """
    The rails used to reverse the funds.
    """
    object: Literal["treasury.credit_reversal"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    received_credit: str
    """
    The ReceivedCredit being reversed.
    """
    status: Literal["canceled", "posted", "processing"]
    """
    Status of the CreditReversal
    """
    status_transitions: StatusTransitions
    transaction: Optional[ExpandableField["Transaction"]]
    """
    The Transaction associated with this object.
    """

    @classmethod
    def create(
        cls, **params: Unpack["CreditReversalCreateParams"]
    ) -> "CreditReversal":
        """
        Reverses a ReceivedCredit and creates a CreditReversal object.
        """
        return cast(
            "CreditReversal",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["CreditReversalCreateParams"]
    ) -> "CreditReversal":
        """
        Reverses a ReceivedCredit and creates a CreditReversal object.
        """
        return cast(
            "CreditReversal",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["CreditReversalListParams"]
    ) -> ListObject["CreditReversal"]:
        """
        Returns a list of CreditReversals.
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
        cls, **params: Unpack["CreditReversalListParams"]
    ) -> ListObject["CreditReversal"]:
        """
        Returns a list of CreditReversals.
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
        cls, id: str, **params: Unpack["CreditReversalRetrieveParams"]
    ) -> "CreditReversal":
        """
        Retrieves the details of an existing CreditReversal by passing the unique CreditReversal ID from either the CreditReversal creation request or CreditReversal list
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["CreditReversalRetrieveParams"]
    ) -> "CreditReversal":
        """
        Retrieves the details of an existing CreditReversal by passing the unique CreditReversal ID from either the CreditReversal creation request or CreditReversal list
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {"status_transitions": StatusTransitions}
