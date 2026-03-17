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
    from stripe.params.treasury._debit_reversal_create_params import (
        DebitReversalCreateParams,
    )
    from stripe.params.treasury._debit_reversal_list_params import (
        DebitReversalListParams,
    )
    from stripe.params.treasury._debit_reversal_retrieve_params import (
        DebitReversalRetrieveParams,
    )
    from stripe.treasury._transaction import Transaction


class DebitReversal(
    CreateableAPIResource["DebitReversal"],
    ListableAPIResource["DebitReversal"],
):
    """
    You can reverse some [ReceivedDebits](https://api.stripe.com#received_debits) depending on their network and source flow. Reversing a ReceivedDebit leads to the creation of a new object known as a DebitReversal.
    """

    OBJECT_NAME: ClassVar[Literal["treasury.debit_reversal"]] = (
        "treasury.debit_reversal"
    )

    class LinkedFlows(StripeObject):
        issuing_dispute: Optional[str]
        """
        Set if there is an Issuing dispute associated with the DebitReversal.
        """

    class StatusTransitions(StripeObject):
        completed_at: Optional[int]
        """
        Timestamp describing when the DebitReversal changed status to `completed`.
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
    financial_account: Optional[str]
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
    linked_flows: Optional[LinkedFlows]
    """
    Other flows linked to a DebitReversal.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    network: Literal["ach", "card"]
    """
    The rails used to reverse the funds.
    """
    object: Literal["treasury.debit_reversal"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    received_debit: str
    """
    The ReceivedDebit being reversed.
    """
    status: Literal["failed", "processing", "succeeded"]
    """
    Status of the DebitReversal
    """
    status_transitions: StatusTransitions
    transaction: Optional[ExpandableField["Transaction"]]
    """
    The Transaction associated with this object.
    """

    @classmethod
    def create(
        cls, **params: Unpack["DebitReversalCreateParams"]
    ) -> "DebitReversal":
        """
        Reverses a ReceivedDebit and creates a DebitReversal object.
        """
        return cast(
            "DebitReversal",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["DebitReversalCreateParams"]
    ) -> "DebitReversal":
        """
        Reverses a ReceivedDebit and creates a DebitReversal object.
        """
        return cast(
            "DebitReversal",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["DebitReversalListParams"]
    ) -> ListObject["DebitReversal"]:
        """
        Returns a list of DebitReversals.
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
        cls, **params: Unpack["DebitReversalListParams"]
    ) -> ListObject["DebitReversal"]:
        """
        Returns a list of DebitReversals.
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
        cls, id: str, **params: Unpack["DebitReversalRetrieveParams"]
    ) -> "DebitReversal":
        """
        Retrieves a DebitReversal object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["DebitReversalRetrieveParams"]
    ) -> "DebitReversal":
        """
        Retrieves a DebitReversal object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "linked_flows": LinkedFlows,
        "status_transitions": StatusTransitions,
    }
