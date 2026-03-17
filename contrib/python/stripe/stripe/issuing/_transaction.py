# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._test_helpers import APIResourceTestHelpers
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Type, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._balance_transaction import BalanceTransaction
    from stripe.issuing._authorization import Authorization
    from stripe.issuing._card import Card
    from stripe.issuing._cardholder import Cardholder
    from stripe.issuing._dispute import Dispute
    from stripe.issuing._token import Token
    from stripe.params.issuing._transaction_create_force_capture_params import (
        TransactionCreateForceCaptureParams,
    )
    from stripe.params.issuing._transaction_create_unlinked_refund_params import (
        TransactionCreateUnlinkedRefundParams,
    )
    from stripe.params.issuing._transaction_list_params import (
        TransactionListParams,
    )
    from stripe.params.issuing._transaction_modify_params import (
        TransactionModifyParams,
    )
    from stripe.params.issuing._transaction_refund_params import (
        TransactionRefundParams,
    )
    from stripe.params.issuing._transaction_retrieve_params import (
        TransactionRetrieveParams,
    )


class Transaction(
    ListableAPIResource["Transaction"],
    UpdateableAPIResource["Transaction"],
):
    """
    Any use of an [issued card](https://docs.stripe.com/issuing) that results in funds entering or leaving
    your Stripe account, such as a completed purchase or refund, is represented by an Issuing
    `Transaction` object.

    Related guide: [Issued card transactions](https://docs.stripe.com/issuing/purchases/transactions)
    """

    OBJECT_NAME: ClassVar[Literal["issuing.transaction"]] = (
        "issuing.transaction"
    )

    class AmountDetails(StripeObject):
        atm_fee: Optional[int]
        """
        The fee charged by the ATM for the cash withdrawal.
        """
        cashback_amount: Optional[int]
        """
        The amount of cash requested by the cardholder.
        """

    class MerchantData(StripeObject):
        category: str
        """
        A categorization of the seller's type of business. See our [merchant categories guide](https://docs.stripe.com/issuing/merchant-categories) for a list of possible values.
        """
        category_code: str
        """
        The merchant category code for the seller's business
        """
        city: Optional[str]
        """
        City where the seller is located
        """
        country: Optional[str]
        """
        Country where the seller is located
        """
        name: Optional[str]
        """
        Name of the seller
        """
        network_id: str
        """
        Identifier assigned to the seller by the card network. Different card networks may assign different network_id fields to the same merchant.
        """
        postal_code: Optional[str]
        """
        Postal code where the seller is located
        """
        state: Optional[str]
        """
        State where the seller is located
        """
        tax_id: Optional[str]
        """
        The seller's tax identification number. Currently populated for French merchants only.
        """
        terminal_id: Optional[str]
        """
        An ID assigned by the seller to the location of the sale.
        """
        url: Optional[str]
        """
        URL provided by the merchant on a 3DS request
        """

    class NetworkData(StripeObject):
        authorization_code: Optional[str]
        """
        A code created by Stripe which is shared with the merchant to validate the authorization. This field will be populated if the authorization message was approved. The code typically starts with the letter "S", followed by a six-digit number. For example, "S498162". Please note that the code is not guaranteed to be unique across authorizations.
        """
        processing_date: Optional[str]
        """
        The date the transaction was processed by the card network. This can be different from the date the seller recorded the transaction depending on when the acquirer submits the transaction to the network.
        """
        transaction_id: Optional[str]
        """
        Unique identifier for the authorization assigned by the card network used to match subsequent messages, disputes, and transactions.
        """

    class PurchaseDetails(StripeObject):
        class Fleet(StripeObject):
            class CardholderPromptData(StripeObject):
                driver_id: Optional[str]
                """
                Driver ID.
                """
                odometer: Optional[int]
                """
                Odometer reading.
                """
                unspecified_id: Optional[str]
                """
                An alphanumeric ID. This field is used when a vehicle ID, driver ID, or generic ID is entered by the cardholder, but the merchant or card network did not specify the prompt type.
                """
                user_id: Optional[str]
                """
                User ID.
                """
                vehicle_number: Optional[str]
                """
                Vehicle number.
                """

            class ReportedBreakdown(StripeObject):
                class Fuel(StripeObject):
                    gross_amount_decimal: Optional[str]
                    """
                    Gross fuel amount that should equal Fuel Volume multipled by Fuel Unit Cost, inclusive of taxes.
                    """

                class NonFuel(StripeObject):
                    gross_amount_decimal: Optional[str]
                    """
                    Gross non-fuel amount that should equal the sum of the line items, inclusive of taxes.
                    """

                class Tax(StripeObject):
                    local_amount_decimal: Optional[str]
                    """
                    Amount of state or provincial Sales Tax included in the transaction amount. Null if not reported by merchant or not subject to tax.
                    """
                    national_amount_decimal: Optional[str]
                    """
                    Amount of national Sales Tax or VAT included in the transaction amount. Null if not reported by merchant or not subject to tax.
                    """

                fuel: Optional[Fuel]
                """
                Breakdown of fuel portion of the purchase.
                """
                non_fuel: Optional[NonFuel]
                """
                Breakdown of non-fuel portion of the purchase.
                """
                tax: Optional[Tax]
                """
                Information about tax included in this transaction.
                """
                _inner_class_types = {
                    "fuel": Fuel,
                    "non_fuel": NonFuel,
                    "tax": Tax,
                }

            cardholder_prompt_data: Optional[CardholderPromptData]
            """
            Answers to prompts presented to cardholder at point of sale.
            """
            purchase_type: Optional[str]
            """
            The type of purchase. One of `fuel_purchase`, `non_fuel_purchase`, or `fuel_and_non_fuel_purchase`.
            """
            reported_breakdown: Optional[ReportedBreakdown]
            """
            More information about the total amount. This information is not guaranteed to be accurate as some merchants may provide unreliable data.
            """
            service_type: Optional[str]
            """
            The type of fuel service. One of `non_fuel_transaction`, `full_service`, or `self_service`.
            """
            _inner_class_types = {
                "cardholder_prompt_data": CardholderPromptData,
                "reported_breakdown": ReportedBreakdown,
            }

        class Flight(StripeObject):
            class Segment(StripeObject):
                arrival_airport_code: Optional[str]
                """
                The three-letter IATA airport code of the flight's destination.
                """
                carrier: Optional[str]
                """
                The airline carrier code.
                """
                departure_airport_code: Optional[str]
                """
                The three-letter IATA airport code that the flight departed from.
                """
                flight_number: Optional[str]
                """
                The flight number.
                """
                service_class: Optional[str]
                """
                The flight's service class.
                """
                stopover_allowed: Optional[bool]
                """
                Whether a stopover is allowed on this flight.
                """

            departure_at: Optional[int]
            """
            The time that the flight departed.
            """
            passenger_name: Optional[str]
            """
            The name of the passenger.
            """
            refundable: Optional[bool]
            """
            Whether the ticket is refundable.
            """
            segments: Optional[List[Segment]]
            """
            The legs of the trip.
            """
            travel_agency: Optional[str]
            """
            The travel agency that issued the ticket.
            """
            _inner_class_types = {"segments": Segment}

        class Fuel(StripeObject):
            industry_product_code: Optional[str]
            """
            [Conexxus Payment System Product Code](https://www.conexxus.org/conexxus-payment-system-product-codes) identifying the primary fuel product purchased.
            """
            quantity_decimal: Optional[str]
            """
            The quantity of `unit`s of fuel that was dispensed, represented as a decimal string with at most 12 decimal places.
            """
            type: str
            """
            The type of fuel that was purchased. One of `diesel`, `unleaded_plus`, `unleaded_regular`, `unleaded_super`, or `other`.
            """
            unit: str
            """
            The units for `quantity_decimal`. One of `charging_minute`, `imperial_gallon`, `kilogram`, `kilowatt_hour`, `liter`, `pound`, `us_gallon`, or `other`.
            """
            unit_cost_decimal: str
            """
            The cost in cents per each unit of fuel, represented as a decimal string with at most 12 decimal places.
            """

        class Lodging(StripeObject):
            check_in_at: Optional[int]
            """
            The time of checking into the lodging.
            """
            nights: Optional[int]
            """
            The number of nights stayed at the lodging.
            """

        class Receipt(StripeObject):
            description: Optional[str]
            """
            The description of the item. The maximum length of this field is 26 characters.
            """
            quantity: Optional[float]
            """
            The quantity of the item.
            """
            total: Optional[int]
            """
            The total for this line item in cents.
            """
            unit_cost: Optional[int]
            """
            The unit cost of the item in cents.
            """

        fleet: Optional[Fleet]
        """
        Fleet-specific information for transactions using Fleet cards.
        """
        flight: Optional[Flight]
        """
        Information about the flight that was purchased with this transaction.
        """
        fuel: Optional[Fuel]
        """
        Information about fuel that was purchased with this transaction.
        """
        lodging: Optional[Lodging]
        """
        Information about lodging that was purchased with this transaction.
        """
        receipt: Optional[List[Receipt]]
        """
        The line items in the purchase.
        """
        reference: Optional[str]
        """
        A merchant-specific order number.
        """
        _inner_class_types = {
            "fleet": Fleet,
            "flight": Flight,
            "fuel": Fuel,
            "lodging": Lodging,
            "receipt": Receipt,
        }

    class Treasury(StripeObject):
        received_credit: Optional[str]
        """
        The Treasury [ReceivedCredit](https://docs.stripe.com/api/treasury/received_credits) representing this Issuing transaction if it is a refund
        """
        received_debit: Optional[str]
        """
        The Treasury [ReceivedDebit](https://docs.stripe.com/api/treasury/received_debits) representing this Issuing transaction if it is a capture
        """

    amount: int
    """
    The transaction amount, which will be reflected in your balance. This amount is in your currency and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    amount_details: Optional[AmountDetails]
    """
    Detailed breakdown of amount components. These amounts are denominated in `currency` and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    authorization: Optional[ExpandableField["Authorization"]]
    """
    The `Authorization` object that led to this transaction.
    """
    balance_transaction: Optional[ExpandableField["BalanceTransaction"]]
    """
    ID of the [balance transaction](https://docs.stripe.com/api/balance_transactions) associated with this transaction.
    """
    card: ExpandableField["Card"]
    """
    The card used to make this transaction.
    """
    cardholder: Optional[ExpandableField["Cardholder"]]
    """
    The cardholder to whom this transaction belongs.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    dispute: Optional[ExpandableField["Dispute"]]
    """
    If you've disputed the transaction, the ID of the dispute.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    merchant_amount: int
    """
    The amount that the merchant will receive, denominated in `merchant_currency` and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal). It will be different from `amount` if the merchant is taking payment in a different currency.
    """
    merchant_currency: str
    """
    The currency with which the merchant is taking payment.
    """
    merchant_data: MerchantData
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    network_data: Optional[NetworkData]
    """
    Details about the transaction, such as processing dates, set by the card network.
    """
    object: Literal["issuing.transaction"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    purchase_details: Optional[PurchaseDetails]
    """
    Additional purchase information that is optionally provided by the merchant.
    """
    token: Optional[ExpandableField["Token"]]
    """
    [Token](https://docs.stripe.com/api/issuing/tokens/object) object used for this transaction. If a network token was not used for this transaction, this field will be null.
    """
    treasury: Optional[Treasury]
    """
    [Treasury](https://docs.stripe.com/api/treasury) details related to this transaction if it was created on a [FinancialAccount](/docs/api/treasury/financial_accounts
    """
    type: Literal["capture", "refund"]
    """
    The nature of the transaction.
    """
    wallet: Optional[Literal["apple_pay", "google_pay", "samsung_pay"]]
    """
    The digital wallet used for this transaction. One of `apple_pay`, `google_pay`, or `samsung_pay`.
    """

    @classmethod
    def list(
        cls, **params: Unpack["TransactionListParams"]
    ) -> ListObject["Transaction"]:
        """
        Returns a list of Issuing Transaction objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
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
        cls, **params: Unpack["TransactionListParams"]
    ) -> ListObject["Transaction"]:
        """
        Returns a list of Issuing Transaction objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
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
        cls, id: str, **params: Unpack["TransactionModifyParams"]
    ) -> "Transaction":
        """
        Updates the specified Issuing Transaction object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Transaction",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["TransactionModifyParams"]
    ) -> "Transaction":
        """
        Updates the specified Issuing Transaction object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Transaction",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["TransactionRetrieveParams"]
    ) -> "Transaction":
        """
        Retrieves an Issuing Transaction object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["TransactionRetrieveParams"]
    ) -> "Transaction":
        """
        Retrieves an Issuing Transaction object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    class TestHelpers(APIResourceTestHelpers["Transaction"]):
        _resource_cls: Type["Transaction"]

        @classmethod
        def create_force_capture(
            cls, **params: Unpack["TransactionCreateForceCaptureParams"]
        ) -> "Transaction":
            """
            Allows the user to capture an arbitrary amount, also known as a forced capture.
            """
            return cast(
                "Transaction",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/transactions/create_force_capture",
                    params=params,
                ),
            )

        @classmethod
        async def create_force_capture_async(
            cls, **params: Unpack["TransactionCreateForceCaptureParams"]
        ) -> "Transaction":
            """
            Allows the user to capture an arbitrary amount, also known as a forced capture.
            """
            return cast(
                "Transaction",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/transactions/create_force_capture",
                    params=params,
                ),
            )

        @classmethod
        def create_unlinked_refund(
            cls, **params: Unpack["TransactionCreateUnlinkedRefundParams"]
        ) -> "Transaction":
            """
            Allows the user to refund an arbitrary amount, also known as a unlinked refund.
            """
            return cast(
                "Transaction",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/transactions/create_unlinked_refund",
                    params=params,
                ),
            )

        @classmethod
        async def create_unlinked_refund_async(
            cls, **params: Unpack["TransactionCreateUnlinkedRefundParams"]
        ) -> "Transaction":
            """
            Allows the user to refund an arbitrary amount, also known as a unlinked refund.
            """
            return cast(
                "Transaction",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/transactions/create_unlinked_refund",
                    params=params,
                ),
            )

        @classmethod
        def _cls_refund(
            cls, transaction: str, **params: Unpack["TransactionRefundParams"]
        ) -> "Transaction":
            """
            Refund a test-mode Transaction.
            """
            return cast(
                "Transaction",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/transactions/{transaction}/refund".format(
                        transaction=sanitize_id(transaction)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def refund(
            transaction: str, **params: Unpack["TransactionRefundParams"]
        ) -> "Transaction":
            """
            Refund a test-mode Transaction.
            """
            ...

        @overload
        def refund(
            self, **params: Unpack["TransactionRefundParams"]
        ) -> "Transaction":
            """
            Refund a test-mode Transaction.
            """
            ...

        @class_method_variant("_cls_refund")
        def refund(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["TransactionRefundParams"]
        ) -> "Transaction":
            """
            Refund a test-mode Transaction.
            """
            return cast(
                "Transaction",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/transactions/{transaction}/refund".format(
                        transaction=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_refund_async(
            cls, transaction: str, **params: Unpack["TransactionRefundParams"]
        ) -> "Transaction":
            """
            Refund a test-mode Transaction.
            """
            return cast(
                "Transaction",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/transactions/{transaction}/refund".format(
                        transaction=sanitize_id(transaction)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def refund_async(
            transaction: str, **params: Unpack["TransactionRefundParams"]
        ) -> "Transaction":
            """
            Refund a test-mode Transaction.
            """
            ...

        @overload
        async def refund_async(
            self, **params: Unpack["TransactionRefundParams"]
        ) -> "Transaction":
            """
            Refund a test-mode Transaction.
            """
            ...

        @class_method_variant("_cls_refund_async")
        async def refund_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["TransactionRefundParams"]
        ) -> "Transaction":
            """
            Refund a test-mode Transaction.
            """
            return cast(
                "Transaction",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/transactions/{transaction}/refund".format(
                        transaction=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

    @property
    def test_helpers(self):
        return self.TestHelpers(self)

    _inner_class_types = {
        "amount_details": AmountDetails,
        "merchant_data": MerchantData,
        "network_data": NetworkData,
        "purchase_details": PurchaseDetails,
        "treasury": Treasury,
    }


Transaction.TestHelpers._resource_cls = Transaction
