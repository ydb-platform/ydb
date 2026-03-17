# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, Optional, Union, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._application_fee import ApplicationFee
    from stripe._balance_transaction import BalanceTransaction
    from stripe._bank_account import BankAccount
    from stripe._card import Card
    from stripe.params._payout_cancel_params import PayoutCancelParams
    from stripe.params._payout_create_params import PayoutCreateParams
    from stripe.params._payout_list_params import PayoutListParams
    from stripe.params._payout_modify_params import PayoutModifyParams
    from stripe.params._payout_retrieve_params import PayoutRetrieveParams
    from stripe.params._payout_reverse_params import PayoutReverseParams


class Payout(
    CreateableAPIResource["Payout"],
    ListableAPIResource["Payout"],
    UpdateableAPIResource["Payout"],
):
    """
    A `Payout` object is created when you receive funds from Stripe, or when you
    initiate a payout to either a bank account or debit card of a [connected
    Stripe account](https://docs.stripe.com/docs/connect/bank-debit-card-payouts). You can retrieve individual payouts,
    and list all payouts. Payouts are made on [varying
    schedules](https://docs.stripe.com/docs/connect/manage-payout-schedule), depending on your country and
    industry.

    Related guide: [Receiving payouts](https://docs.stripe.com/payouts)
    """

    OBJECT_NAME: ClassVar[Literal["payout"]] = "payout"

    class TraceId(StripeObject):
        status: str
        """
        Possible values are `pending`, `supported`, and `unsupported`. When `payout.status` is `pending` or `in_transit`, this will be `pending`. When the payout transitions to `paid`, `failed`, or `canceled`, this status will become `supported` or `unsupported` shortly after in most cases. In some cases, this may appear as `pending` for up to 10 days after `arrival_date` until transitioning to `supported` or `unsupported`.
        """
        value: Optional[str]
        """
        The trace ID value if `trace_id.status` is `supported`, otherwise `nil`.
        """

    amount: int
    """
    The amount (in cents (or local equivalent)) that transfers to your bank account or debit card.
    """
    application_fee: Optional[ExpandableField["ApplicationFee"]]
    """
    The application fee (if any) for the payout. [See the Connect documentation](https://docs.stripe.com/connect/instant-payouts#monetization-and-fees) for details.
    """
    application_fee_amount: Optional[int]
    """
    The amount of the application fee (if any) requested for the payout. [See the Connect documentation](https://docs.stripe.com/connect/instant-payouts#monetization-and-fees) for details.
    """
    arrival_date: int
    """
    Date that you can expect the payout to arrive in the bank. This factors in delays to account for weekends or bank holidays.
    """
    automatic: bool
    """
    Returns `true` if the payout is created by an [automated payout schedule](https://docs.stripe.com/payouts#payout-schedule) and `false` if it's [requested manually](https://stripe.com/docs/payouts#manual-payouts).
    """
    balance_transaction: Optional[ExpandableField["BalanceTransaction"]]
    """
    ID of the balance transaction that describes the impact of this payout on your account balance.
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
    destination: Optional[ExpandableField[Union["BankAccount", "Card"]]]
    """
    ID of the bank account or card the payout is sent to.
    """
    failure_balance_transaction: Optional[
        ExpandableField["BalanceTransaction"]
    ]
    """
    If the payout fails or cancels, this is the ID of the balance transaction that reverses the initial balance transaction and returns the funds from the failed payout back in your balance.
    """
    failure_code: Optional[str]
    """
    Error code that provides a reason for a payout failure, if available. View our [list of failure codes](https://docs.stripe.com/api#payout_failures).
    """
    failure_message: Optional[str]
    """
    Message that provides the reason for a payout failure, if available.
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
    method: str
    """
    The method used to send this payout, which can be `standard` or `instant`. `instant` is supported for payouts to debit cards and bank accounts in certain countries. Learn more about [bank support for Instant Payouts](https://stripe.com/docs/payouts/instant-payouts-banks).
    """
    object: Literal["payout"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    original_payout: Optional[ExpandableField["Payout"]]
    """
    If the payout reverses another, this is the ID of the original payout.
    """
    payout_method: Optional[str]
    """
    ID of the v2 FinancialAccount the funds are sent to.
    """
    reconciliation_status: Literal[
        "completed", "in_progress", "not_applicable"
    ]
    """
    If `completed`, you can use the [Balance Transactions API](https://docs.stripe.com/api/balance_transactions/list#balance_transaction_list-payout) to list all balance transactions that are paid out in this payout.
    """
    reversed_by: Optional[ExpandableField["Payout"]]
    """
    If the payout reverses, this is the ID of the payout that reverses this payout.
    """
    source_type: str
    """
    The source balance this payout came from, which can be one of the following: `card`, `fpx`, or `bank_account`.
    """
    statement_descriptor: Optional[str]
    """
    Extra information about a payout that displays on the user's bank statement.
    """
    status: str
    """
    Current status of the payout: `paid`, `pending`, `in_transit`, `canceled` or `failed`. A payout is `pending` until it's submitted to the bank, when it becomes `in_transit`. The status changes to `paid` if the transaction succeeds, or to `failed` or `canceled` (within 5 business days). Some payouts that fail might initially show as `paid`, then change to `failed`.
    """
    trace_id: Optional[TraceId]
    """
    A value that generates from the beneficiary's bank that allows users to track payouts with their bank. Banks might call this a "reference number" or something similar.
    """
    type: Literal["bank_account", "card"]
    """
    Can be `bank_account` or `card`.
    """

    @classmethod
    def _cls_cancel(
        cls, payout: str, **params: Unpack["PayoutCancelParams"]
    ) -> "Payout":
        """
        You can cancel a previously created payout if its status is pending. Stripe refunds the funds to your available balance. You can't cancel automatic Stripe payouts.
        """
        return cast(
            "Payout",
            cls._static_request(
                "post",
                "/v1/payouts/{payout}/cancel".format(
                    payout=sanitize_id(payout)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def cancel(
        payout: str, **params: Unpack["PayoutCancelParams"]
    ) -> "Payout":
        """
        You can cancel a previously created payout if its status is pending. Stripe refunds the funds to your available balance. You can't cancel automatic Stripe payouts.
        """
        ...

    @overload
    def cancel(self, **params: Unpack["PayoutCancelParams"]) -> "Payout":
        """
        You can cancel a previously created payout if its status is pending. Stripe refunds the funds to your available balance. You can't cancel automatic Stripe payouts.
        """
        ...

    @class_method_variant("_cls_cancel")
    def cancel(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PayoutCancelParams"]
    ) -> "Payout":
        """
        You can cancel a previously created payout if its status is pending. Stripe refunds the funds to your available balance. You can't cancel automatic Stripe payouts.
        """
        return cast(
            "Payout",
            self._request(
                "post",
                "/v1/payouts/{payout}/cancel".format(
                    payout=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_cancel_async(
        cls, payout: str, **params: Unpack["PayoutCancelParams"]
    ) -> "Payout":
        """
        You can cancel a previously created payout if its status is pending. Stripe refunds the funds to your available balance. You can't cancel automatic Stripe payouts.
        """
        return cast(
            "Payout",
            await cls._static_request_async(
                "post",
                "/v1/payouts/{payout}/cancel".format(
                    payout=sanitize_id(payout)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def cancel_async(
        payout: str, **params: Unpack["PayoutCancelParams"]
    ) -> "Payout":
        """
        You can cancel a previously created payout if its status is pending. Stripe refunds the funds to your available balance. You can't cancel automatic Stripe payouts.
        """
        ...

    @overload
    async def cancel_async(
        self, **params: Unpack["PayoutCancelParams"]
    ) -> "Payout":
        """
        You can cancel a previously created payout if its status is pending. Stripe refunds the funds to your available balance. You can't cancel automatic Stripe payouts.
        """
        ...

    @class_method_variant("_cls_cancel_async")
    async def cancel_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PayoutCancelParams"]
    ) -> "Payout":
        """
        You can cancel a previously created payout if its status is pending. Stripe refunds the funds to your available balance. You can't cancel automatic Stripe payouts.
        """
        return cast(
            "Payout",
            await self._request_async(
                "post",
                "/v1/payouts/{payout}/cancel".format(
                    payout=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(cls, **params: Unpack["PayoutCreateParams"]) -> "Payout":
        """
        To send funds to your own bank account, create a new payout object. Your [Stripe balance](https://docs.stripe.com/api#balance) must cover the payout amount. If it doesn't, you receive an “Insufficient Funds” error.

        If your API key is in test mode, money won't actually be sent, though every other action occurs as if you're in live mode.

        If you create a manual payout on a Stripe account that uses multiple payment source types, you need to specify the source type balance that the payout draws from. The [balance object](https://docs.stripe.com/api#balance_object) details available and pending amounts by source type.
        """
        return cast(
            "Payout",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["PayoutCreateParams"]
    ) -> "Payout":
        """
        To send funds to your own bank account, create a new payout object. Your [Stripe balance](https://docs.stripe.com/api#balance) must cover the payout amount. If it doesn't, you receive an “Insufficient Funds” error.

        If your API key is in test mode, money won't actually be sent, though every other action occurs as if you're in live mode.

        If you create a manual payout on a Stripe account that uses multiple payment source types, you need to specify the source type balance that the payout draws from. The [balance object](https://docs.stripe.com/api#balance_object) details available and pending amounts by source type.
        """
        return cast(
            "Payout",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["PayoutListParams"]
    ) -> ListObject["Payout"]:
        """
        Returns a list of existing payouts sent to third-party bank accounts or payouts that Stripe sent to you. The payouts return in sorted order, with the most recently created payouts appearing first.
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
        cls, **params: Unpack["PayoutListParams"]
    ) -> ListObject["Payout"]:
        """
        Returns a list of existing payouts sent to third-party bank accounts or payouts that Stripe sent to you. The payouts return in sorted order, with the most recently created payouts appearing first.
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
    def modify(
        cls, id: str, **params: Unpack["PayoutModifyParams"]
    ) -> "Payout":
        """
        Updates the specified payout by setting the values of the parameters you pass. We don't change parameters that you don't provide. This request only accepts the metadata as arguments.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Payout",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["PayoutModifyParams"]
    ) -> "Payout":
        """
        Updates the specified payout by setting the values of the parameters you pass. We don't change parameters that you don't provide. This request only accepts the metadata as arguments.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Payout",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["PayoutRetrieveParams"]
    ) -> "Payout":
        """
        Retrieves the details of an existing payout. Supply the unique payout ID from either a payout creation request or the payout list. Stripe returns the corresponding payout information.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["PayoutRetrieveParams"]
    ) -> "Payout":
        """
        Retrieves the details of an existing payout. Supply the unique payout ID from either a payout creation request or the payout list. Stripe returns the corresponding payout information.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def _cls_reverse(
        cls, payout: str, **params: Unpack["PayoutReverseParams"]
    ) -> "Payout":
        """
        Reverses a payout by debiting the destination bank account. At this time, you can only reverse payouts for connected accounts to US and Canadian bank accounts. If the payout is manual and in the pending status, use /v1/payouts/:id/cancel instead.

        By requesting a reversal through /v1/payouts/:id/reverse, you confirm that the authorized signatory of the selected bank account authorizes the debit on the bank account and that no other authorization is required.
        """
        return cast(
            "Payout",
            cls._static_request(
                "post",
                "/v1/payouts/{payout}/reverse".format(
                    payout=sanitize_id(payout)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def reverse(
        payout: str, **params: Unpack["PayoutReverseParams"]
    ) -> "Payout":
        """
        Reverses a payout by debiting the destination bank account. At this time, you can only reverse payouts for connected accounts to US and Canadian bank accounts. If the payout is manual and in the pending status, use /v1/payouts/:id/cancel instead.

        By requesting a reversal through /v1/payouts/:id/reverse, you confirm that the authorized signatory of the selected bank account authorizes the debit on the bank account and that no other authorization is required.
        """
        ...

    @overload
    def reverse(self, **params: Unpack["PayoutReverseParams"]) -> "Payout":
        """
        Reverses a payout by debiting the destination bank account. At this time, you can only reverse payouts for connected accounts to US and Canadian bank accounts. If the payout is manual and in the pending status, use /v1/payouts/:id/cancel instead.

        By requesting a reversal through /v1/payouts/:id/reverse, you confirm that the authorized signatory of the selected bank account authorizes the debit on the bank account and that no other authorization is required.
        """
        ...

    @class_method_variant("_cls_reverse")
    def reverse(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PayoutReverseParams"]
    ) -> "Payout":
        """
        Reverses a payout by debiting the destination bank account. At this time, you can only reverse payouts for connected accounts to US and Canadian bank accounts. If the payout is manual and in the pending status, use /v1/payouts/:id/cancel instead.

        By requesting a reversal through /v1/payouts/:id/reverse, you confirm that the authorized signatory of the selected bank account authorizes the debit on the bank account and that no other authorization is required.
        """
        return cast(
            "Payout",
            self._request(
                "post",
                "/v1/payouts/{payout}/reverse".format(
                    payout=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_reverse_async(
        cls, payout: str, **params: Unpack["PayoutReverseParams"]
    ) -> "Payout":
        """
        Reverses a payout by debiting the destination bank account. At this time, you can only reverse payouts for connected accounts to US and Canadian bank accounts. If the payout is manual and in the pending status, use /v1/payouts/:id/cancel instead.

        By requesting a reversal through /v1/payouts/:id/reverse, you confirm that the authorized signatory of the selected bank account authorizes the debit on the bank account and that no other authorization is required.
        """
        return cast(
            "Payout",
            await cls._static_request_async(
                "post",
                "/v1/payouts/{payout}/reverse".format(
                    payout=sanitize_id(payout)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def reverse_async(
        payout: str, **params: Unpack["PayoutReverseParams"]
    ) -> "Payout":
        """
        Reverses a payout by debiting the destination bank account. At this time, you can only reverse payouts for connected accounts to US and Canadian bank accounts. If the payout is manual and in the pending status, use /v1/payouts/:id/cancel instead.

        By requesting a reversal through /v1/payouts/:id/reverse, you confirm that the authorized signatory of the selected bank account authorizes the debit on the bank account and that no other authorization is required.
        """
        ...

    @overload
    async def reverse_async(
        self, **params: Unpack["PayoutReverseParams"]
    ) -> "Payout":
        """
        Reverses a payout by debiting the destination bank account. At this time, you can only reverse payouts for connected accounts to US and Canadian bank accounts. If the payout is manual and in the pending status, use /v1/payouts/:id/cancel instead.

        By requesting a reversal through /v1/payouts/:id/reverse, you confirm that the authorized signatory of the selected bank account authorizes the debit on the bank account and that no other authorization is required.
        """
        ...

    @class_method_variant("_cls_reverse_async")
    async def reverse_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PayoutReverseParams"]
    ) -> "Payout":
        """
        Reverses a payout by debiting the destination bank account. At this time, you can only reverse payouts for connected accounts to US and Canadian bank accounts. If the payout is manual and in the pending status, use /v1/payouts/:id/cancel instead.

        By requesting a reversal through /v1/payouts/:id/reverse, you confirm that the authorized signatory of the selected bank account authorizes the debit on the bank account and that no other authorization is required.
        """
        return cast(
            "Payout",
            await self._request_async(
                "post",
                "/v1/payouts/{payout}/reverse".format(
                    payout=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    _inner_class_types = {"trace_id": TraceId}
