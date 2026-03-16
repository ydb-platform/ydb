# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._singleton_api_resource import SingletonAPIResource
from stripe._stripe_object import StripeObject
from typing import ClassVar, List, Optional
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params._balance_retrieve_params import BalanceRetrieveParams


class Balance(SingletonAPIResource["Balance"]):
    """
    This is an object representing your Stripe balance. You can retrieve it to see
    the balance currently on your Stripe account.

    The top-level `available` and `pending` comprise your "payments balance."

    Related guide: [Balances and settlement time](https://docs.stripe.com/payments/balances), [Understanding Connect account balances](https://docs.stripe.com/connect/account-balances)
    """

    OBJECT_NAME: ClassVar[Literal["balance"]] = "balance"

    class Available(StripeObject):
        class SourceTypes(StripeObject):
            bank_account: Optional[int]
            """
            Amount coming from [legacy US ACH payments](https://docs.stripe.com/ach-deprecated).
            """
            card: Optional[int]
            """
            Amount coming from most payment methods, including cards as well as [non-legacy bank debits](https://docs.stripe.com/payments/bank-debits).
            """
            fpx: Optional[int]
            """
            Amount coming from [FPX](https://docs.stripe.com/payments/fpx), a Malaysian payment method.
            """

        amount: int
        """
        Balance amount.
        """
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        source_types: Optional[SourceTypes]
        _inner_class_types = {"source_types": SourceTypes}

    class ConnectReserved(StripeObject):
        class SourceTypes(StripeObject):
            bank_account: Optional[int]
            """
            Amount coming from [legacy US ACH payments](https://docs.stripe.com/ach-deprecated).
            """
            card: Optional[int]
            """
            Amount coming from most payment methods, including cards as well as [non-legacy bank debits](https://docs.stripe.com/payments/bank-debits).
            """
            fpx: Optional[int]
            """
            Amount coming from [FPX](https://docs.stripe.com/payments/fpx), a Malaysian payment method.
            """

        amount: int
        """
        Balance amount.
        """
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        source_types: Optional[SourceTypes]
        _inner_class_types = {"source_types": SourceTypes}

    class InstantAvailable(StripeObject):
        class NetAvailable(StripeObject):
            class SourceTypes(StripeObject):
                bank_account: Optional[int]
                """
                Amount coming from [legacy US ACH payments](https://docs.stripe.com/ach-deprecated).
                """
                card: Optional[int]
                """
                Amount coming from most payment methods, including cards as well as [non-legacy bank debits](https://docs.stripe.com/payments/bank-debits).
                """
                fpx: Optional[int]
                """
                Amount coming from [FPX](https://docs.stripe.com/payments/fpx), a Malaysian payment method.
                """

            amount: int
            """
            Net balance amount, subtracting fees from platform-set pricing.
            """
            destination: str
            """
            ID of the external account for this net balance (not expandable).
            """
            source_types: Optional[SourceTypes]
            _inner_class_types = {"source_types": SourceTypes}

        class SourceTypes(StripeObject):
            bank_account: Optional[int]
            """
            Amount coming from [legacy US ACH payments](https://docs.stripe.com/ach-deprecated).
            """
            card: Optional[int]
            """
            Amount coming from most payment methods, including cards as well as [non-legacy bank debits](https://docs.stripe.com/payments/bank-debits).
            """
            fpx: Optional[int]
            """
            Amount coming from [FPX](https://docs.stripe.com/payments/fpx), a Malaysian payment method.
            """

        amount: int
        """
        Balance amount.
        """
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        net_available: Optional[List[NetAvailable]]
        """
        Breakdown of balance by destination.
        """
        source_types: Optional[SourceTypes]
        _inner_class_types = {
            "net_available": NetAvailable,
            "source_types": SourceTypes,
        }

    class Issuing(StripeObject):
        class Available(StripeObject):
            class SourceTypes(StripeObject):
                bank_account: Optional[int]
                """
                Amount coming from [legacy US ACH payments](https://docs.stripe.com/ach-deprecated).
                """
                card: Optional[int]
                """
                Amount coming from most payment methods, including cards as well as [non-legacy bank debits](https://docs.stripe.com/payments/bank-debits).
                """
                fpx: Optional[int]
                """
                Amount coming from [FPX](https://docs.stripe.com/payments/fpx), a Malaysian payment method.
                """

            amount: int
            """
            Balance amount.
            """
            currency: str
            """
            Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
            """
            source_types: Optional[SourceTypes]
            _inner_class_types = {"source_types": SourceTypes}

        available: List[Available]
        """
        Funds that are available for use.
        """
        _inner_class_types = {"available": Available}

    class Pending(StripeObject):
        class SourceTypes(StripeObject):
            bank_account: Optional[int]
            """
            Amount coming from [legacy US ACH payments](https://docs.stripe.com/ach-deprecated).
            """
            card: Optional[int]
            """
            Amount coming from most payment methods, including cards as well as [non-legacy bank debits](https://docs.stripe.com/payments/bank-debits).
            """
            fpx: Optional[int]
            """
            Amount coming from [FPX](https://docs.stripe.com/payments/fpx), a Malaysian payment method.
            """

        amount: int
        """
        Balance amount.
        """
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        source_types: Optional[SourceTypes]
        _inner_class_types = {"source_types": SourceTypes}

    class RefundAndDisputePrefunding(StripeObject):
        class Available(StripeObject):
            class SourceTypes(StripeObject):
                bank_account: Optional[int]
                """
                Amount coming from [legacy US ACH payments](https://docs.stripe.com/ach-deprecated).
                """
                card: Optional[int]
                """
                Amount coming from most payment methods, including cards as well as [non-legacy bank debits](https://docs.stripe.com/payments/bank-debits).
                """
                fpx: Optional[int]
                """
                Amount coming from [FPX](https://docs.stripe.com/payments/fpx), a Malaysian payment method.
                """

            amount: int
            """
            Balance amount.
            """
            currency: str
            """
            Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
            """
            source_types: Optional[SourceTypes]
            _inner_class_types = {"source_types": SourceTypes}

        class Pending(StripeObject):
            class SourceTypes(StripeObject):
                bank_account: Optional[int]
                """
                Amount coming from [legacy US ACH payments](https://docs.stripe.com/ach-deprecated).
                """
                card: Optional[int]
                """
                Amount coming from most payment methods, including cards as well as [non-legacy bank debits](https://docs.stripe.com/payments/bank-debits).
                """
                fpx: Optional[int]
                """
                Amount coming from [FPX](https://docs.stripe.com/payments/fpx), a Malaysian payment method.
                """

            amount: int
            """
            Balance amount.
            """
            currency: str
            """
            Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
            """
            source_types: Optional[SourceTypes]
            _inner_class_types = {"source_types": SourceTypes}

        available: List[Available]
        """
        Funds that are available for use.
        """
        pending: List[Pending]
        """
        Funds that are pending
        """
        _inner_class_types = {"available": Available, "pending": Pending}

    available: List[Available]
    """
    Available funds that you can transfer or pay out automatically by Stripe or explicitly through the [Transfers API](https://api.stripe.com#transfers) or [Payouts API](https://api.stripe.com#payouts). You can find the available balance for each currency and payment type in the `source_types` property.
    """
    connect_reserved: Optional[List[ConnectReserved]]
    """
    Funds held due to negative balances on connected accounts where [account.controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `application`, which includes Custom accounts. You can find the connect reserve balance for each currency and payment type in the `source_types` property.
    """
    instant_available: Optional[List[InstantAvailable]]
    """
    Funds that you can pay out using Instant Payouts.
    """
    issuing: Optional[Issuing]
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["balance"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    pending: List[Pending]
    """
    Funds that aren't available in the balance yet. You can find the pending balance for each currency and each payment type in the `source_types` property.
    """
    refund_and_dispute_prefunding: Optional[RefundAndDisputePrefunding]

    @classmethod
    def retrieve(cls, **params: Unpack["BalanceRetrieveParams"]) -> "Balance":
        """
        Retrieves the current account balance, based on the authentication that was used to make the request.
         For a sample request, see [Accounting for negative balances](https://docs.stripe.com/docs/connect/account-balances#accounting-for-negative-balances).
        """
        instance = cls(None, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, **params: Unpack["BalanceRetrieveParams"]
    ) -> "Balance":
        """
        Retrieves the current account balance, based on the authentication that was used to make the request.
         For a sample request, see [Accounting for negative balances](https://docs.stripe.com/docs/connect/account-balances#accounting-for-negative-balances).
        """
        instance = cls(None, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def class_url(cls):
        return "/v1/balance"

    _inner_class_types = {
        "available": Available,
        "connect_reserved": ConnectReserved,
        "instant_available": InstantAvailable,
        "issuing": Issuing,
        "pending": Pending,
        "refund_and_dispute_prefunding": RefundAndDisputePrefunding,
    }
