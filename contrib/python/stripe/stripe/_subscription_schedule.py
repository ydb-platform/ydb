# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._account import Account
    from stripe._application import Application
    from stripe._coupon import Coupon
    from stripe._customer import Customer
    from stripe._discount import Discount as DiscountResource
    from stripe._payment_method import PaymentMethod
    from stripe._plan import Plan
    from stripe._price import Price
    from stripe._promotion_code import PromotionCode
    from stripe._subscription import Subscription
    from stripe._tax_id import TaxId
    from stripe._tax_rate import TaxRate
    from stripe.params._subscription_schedule_cancel_params import (
        SubscriptionScheduleCancelParams,
    )
    from stripe.params._subscription_schedule_create_params import (
        SubscriptionScheduleCreateParams,
    )
    from stripe.params._subscription_schedule_list_params import (
        SubscriptionScheduleListParams,
    )
    from stripe.params._subscription_schedule_modify_params import (
        SubscriptionScheduleModifyParams,
    )
    from stripe.params._subscription_schedule_release_params import (
        SubscriptionScheduleReleaseParams,
    )
    from stripe.params._subscription_schedule_retrieve_params import (
        SubscriptionScheduleRetrieveParams,
    )
    from stripe.test_helpers._test_clock import TestClock


class SubscriptionSchedule(
    CreateableAPIResource["SubscriptionSchedule"],
    ListableAPIResource["SubscriptionSchedule"],
    UpdateableAPIResource["SubscriptionSchedule"],
):
    """
    A subscription schedule allows you to create and manage the lifecycle of a subscription by predefining expected changes.

    Related guide: [Subscription schedules](https://docs.stripe.com/billing/subscriptions/subscription-schedules)
    """

    OBJECT_NAME: ClassVar[Literal["subscription_schedule"]] = (
        "subscription_schedule"
    )

    class BillingMode(StripeObject):
        class Flexible(StripeObject):
            proration_discounts: Optional[Literal["included", "itemized"]]
            """
            Controls how invoices and invoice items display proration amounts and discount amounts.
            """

        flexible: Optional[Flexible]
        """
        Configure behavior for flexible billing mode
        """
        type: Literal["classic", "flexible"]
        """
        Controls how prorations and invoices for subscriptions are calculated and orchestrated.
        """
        updated_at: Optional[int]
        """
        Details on when the current billing_mode was adopted.
        """
        _inner_class_types = {"flexible": Flexible}

    class CurrentPhase(StripeObject):
        end_date: int
        """
        The end of this phase of the subscription schedule.
        """
        start_date: int
        """
        The start of this phase of the subscription schedule.
        """

    class DefaultSettings(StripeObject):
        class AutomaticTax(StripeObject):
            class Liability(StripeObject):
                account: Optional[ExpandableField["Account"]]
                """
                The connected account being referenced when `type` is `account`.
                """
                type: Literal["account", "self"]
                """
                Type of the account referenced.
                """

            disabled_reason: Optional[Literal["requires_location_inputs"]]
            """
            If Stripe disabled automatic tax, this enum describes why.
            """
            enabled: bool
            """
            Whether Stripe automatically computes tax on invoices created during this phase.
            """
            liability: Optional[Liability]
            """
            The account that's liable for tax. If set, the business address and tax registrations required to perform the tax calculation are loaded from this account. The tax transaction is returned in the report of the connected account.
            """
            _inner_class_types = {"liability": Liability}

        class BillingThresholds(StripeObject):
            amount_gte: Optional[int]
            """
            Monetary threshold that triggers the subscription to create an invoice
            """
            reset_billing_cycle_anchor: Optional[bool]
            """
            Indicates if the `billing_cycle_anchor` should be reset when a threshold is reached. If true, `billing_cycle_anchor` will be updated to the date/time the threshold was last reached; otherwise, the value will remain unchanged. This value may not be `true` if the subscription contains items with plans that have `aggregate_usage=last_ever`.
            """

        class InvoiceSettings(StripeObject):
            class Issuer(StripeObject):
                account: Optional[ExpandableField["Account"]]
                """
                The connected account being referenced when `type` is `account`.
                """
                type: Literal["account", "self"]
                """
                Type of the account referenced.
                """

            account_tax_ids: Optional[List[ExpandableField["TaxId"]]]
            """
            The account tax IDs associated with the subscription schedule. Will be set on invoices generated by the subscription schedule.
            """
            days_until_due: Optional[int]
            """
            Number of days within which a customer must pay invoices generated by this subscription schedule. This value will be `null` for subscription schedules where `billing=charge_automatically`.
            """
            issuer: Issuer
            _inner_class_types = {"issuer": Issuer}

        class TransferData(StripeObject):
            amount_percent: Optional[float]
            """
            A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the destination account. By default, the entire amount is transferred to the destination.
            """
            destination: ExpandableField["Account"]
            """
            The account where funds from the payment will be transferred to upon payment success.
            """

        application_fee_percent: Optional[float]
        """
        A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the application owner's Stripe account during this phase of the schedule.
        """
        automatic_tax: Optional[AutomaticTax]
        billing_cycle_anchor: Literal["automatic", "phase_start"]
        """
        Possible values are `phase_start` or `automatic`. If `phase_start` then billing cycle anchor of the subscription is set to the start of the phase when entering the phase. If `automatic` then the billing cycle anchor is automatically modified as needed when entering the phase. For more information, see the billing cycle [documentation](https://docs.stripe.com/billing/subscriptions/billing-cycle).
        """
        billing_thresholds: Optional[BillingThresholds]
        """
        Define thresholds at which an invoice will be sent, and the subscription advanced to a new billing period
        """
        collection_method: Optional[
            Literal["charge_automatically", "send_invoice"]
        ]
        """
        Either `charge_automatically`, or `send_invoice`. When charging automatically, Stripe will attempt to pay the underlying subscription at the end of each billing cycle using the default source attached to the customer. When sending an invoice, Stripe will email your customer an invoice with payment instructions and mark the subscription as `active`.
        """
        default_payment_method: Optional[ExpandableField["PaymentMethod"]]
        """
        ID of the default payment method for the subscription schedule. If not set, invoices will use the default payment method in the customer's invoice settings.
        """
        description: Optional[str]
        """
        Subscription description, meant to be displayable to the customer. Use this field to optionally store an explanation of the subscription for rendering in Stripe surfaces and certain local payment methods UIs.
        """
        invoice_settings: InvoiceSettings
        on_behalf_of: Optional[ExpandableField["Account"]]
        """
        The account (if any) the charge was made on behalf of for charges associated with the schedule's subscription. See the Connect documentation for details.
        """
        transfer_data: Optional[TransferData]
        """
        The account (if any) the associated subscription's payments will be attributed to for tax reporting, and where funds from each payment will be transferred to for each of the subscription's invoices.
        """
        _inner_class_types = {
            "automatic_tax": AutomaticTax,
            "billing_thresholds": BillingThresholds,
            "invoice_settings": InvoiceSettings,
            "transfer_data": TransferData,
        }

    class Phase(StripeObject):
        class AddInvoiceItem(StripeObject):
            class Discount(StripeObject):
                coupon: Optional[ExpandableField["Coupon"]]
                """
                ID of the coupon to create a new discount for.
                """
                discount: Optional[ExpandableField["DiscountResource"]]
                """
                ID of an existing discount on the object (or one of its ancestors) to reuse.
                """
                promotion_code: Optional[ExpandableField["PromotionCode"]]
                """
                ID of the promotion code to create a new discount for.
                """

            class Period(StripeObject):
                class End(StripeObject):
                    timestamp: Optional[int]
                    """
                    A precise Unix timestamp for the end of the invoice item period. Must be greater than or equal to `period.start`.
                    """
                    type: Literal[
                        "min_item_period_end", "phase_end", "timestamp"
                    ]
                    """
                    Select how to calculate the end of the invoice item period.
                    """

                class Start(StripeObject):
                    timestamp: Optional[int]
                    """
                    A precise Unix timestamp for the start of the invoice item period. Must be less than or equal to `period.end`.
                    """
                    type: Literal[
                        "max_item_period_start", "phase_start", "timestamp"
                    ]
                    """
                    Select how to calculate the start of the invoice item period.
                    """

                end: End
                start: Start
                _inner_class_types = {"end": End, "start": Start}

            discounts: List[Discount]
            """
            The stackable discounts that will be applied to the item.
            """
            metadata: Optional[Dict[str, str]]
            """
            Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
            """
            period: Period
            price: ExpandableField["Price"]
            """
            ID of the price used to generate the invoice item.
            """
            quantity: Optional[int]
            """
            The quantity of the invoice item.
            """
            tax_rates: Optional[List["TaxRate"]]
            """
            The tax rates which apply to the item. When set, the `default_tax_rates` do not apply to this item.
            """
            _inner_class_types = {"discounts": Discount, "period": Period}

        class AutomaticTax(StripeObject):
            class Liability(StripeObject):
                account: Optional[ExpandableField["Account"]]
                """
                The connected account being referenced when `type` is `account`.
                """
                type: Literal["account", "self"]
                """
                Type of the account referenced.
                """

            disabled_reason: Optional[Literal["requires_location_inputs"]]
            """
            If Stripe disabled automatic tax, this enum describes why.
            """
            enabled: bool
            """
            Whether Stripe automatically computes tax on invoices created during this phase.
            """
            liability: Optional[Liability]
            """
            The account that's liable for tax. If set, the business address and tax registrations required to perform the tax calculation are loaded from this account. The tax transaction is returned in the report of the connected account.
            """
            _inner_class_types = {"liability": Liability}

        class BillingThresholds(StripeObject):
            amount_gte: Optional[int]
            """
            Monetary threshold that triggers the subscription to create an invoice
            """
            reset_billing_cycle_anchor: Optional[bool]
            """
            Indicates if the `billing_cycle_anchor` should be reset when a threshold is reached. If true, `billing_cycle_anchor` will be updated to the date/time the threshold was last reached; otherwise, the value will remain unchanged. This value may not be `true` if the subscription contains items with plans that have `aggregate_usage=last_ever`.
            """

        class Discount(StripeObject):
            coupon: Optional[ExpandableField["Coupon"]]
            """
            ID of the coupon to create a new discount for.
            """
            discount: Optional[ExpandableField["DiscountResource"]]
            """
            ID of an existing discount on the object (or one of its ancestors) to reuse.
            """
            promotion_code: Optional[ExpandableField["PromotionCode"]]
            """
            ID of the promotion code to create a new discount for.
            """

        class InvoiceSettings(StripeObject):
            class Issuer(StripeObject):
                account: Optional[ExpandableField["Account"]]
                """
                The connected account being referenced when `type` is `account`.
                """
                type: Literal["account", "self"]
                """
                Type of the account referenced.
                """

            account_tax_ids: Optional[List[ExpandableField["TaxId"]]]
            """
            The account tax IDs associated with this phase of the subscription schedule. Will be set on invoices generated by this phase of the subscription schedule.
            """
            days_until_due: Optional[int]
            """
            Number of days within which a customer must pay invoices generated by this subscription schedule. This value will be `null` for subscription schedules where `billing=charge_automatically`.
            """
            issuer: Optional[Issuer]
            """
            The connected account that issues the invoice. The invoice is presented with the branding and support information of the specified account.
            """
            _inner_class_types = {"issuer": Issuer}

        class Item(StripeObject):
            class BillingThresholds(StripeObject):
                usage_gte: Optional[int]
                """
                Usage threshold that triggers the subscription to create an invoice
                """

            class Discount(StripeObject):
                coupon: Optional[ExpandableField["Coupon"]]
                """
                ID of the coupon to create a new discount for.
                """
                discount: Optional[ExpandableField["DiscountResource"]]
                """
                ID of an existing discount on the object (or one of its ancestors) to reuse.
                """
                promotion_code: Optional[ExpandableField["PromotionCode"]]
                """
                ID of the promotion code to create a new discount for.
                """

            billing_thresholds: Optional[BillingThresholds]
            """
            Define thresholds at which an invoice will be sent, and the related subscription advanced to a new billing period
            """
            discounts: List[Discount]
            """
            The discounts applied to the subscription item. Subscription item discounts are applied before subscription discounts. Use `expand[]=discounts` to expand each discount.
            """
            metadata: Optional[Dict[str, str]]
            """
            Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an item. Metadata on this item will update the underlying subscription item's `metadata` when the phase is entered.
            """
            plan: ExpandableField["Plan"]
            """
            ID of the plan to which the customer should be subscribed.
            """
            price: ExpandableField["Price"]
            """
            ID of the price to which the customer should be subscribed.
            """
            quantity: Optional[int]
            """
            Quantity of the plan to which the customer should be subscribed.
            """
            tax_rates: Optional[List["TaxRate"]]
            """
            The tax rates which apply to this `phase_item`. When set, the `default_tax_rates` on the phase do not apply to this `phase_item`.
            """
            _inner_class_types = {
                "billing_thresholds": BillingThresholds,
                "discounts": Discount,
            }

        class TransferData(StripeObject):
            amount_percent: Optional[float]
            """
            A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the destination account. By default, the entire amount is transferred to the destination.
            """
            destination: ExpandableField["Account"]
            """
            The account where funds from the payment will be transferred to upon payment success.
            """

        add_invoice_items: List[AddInvoiceItem]
        """
        A list of prices and quantities that will generate invoice items appended to the next invoice for this phase.
        """
        application_fee_percent: Optional[float]
        """
        A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the application owner's Stripe account during this phase of the schedule.
        """
        automatic_tax: Optional[AutomaticTax]
        billing_cycle_anchor: Optional[Literal["automatic", "phase_start"]]
        """
        Possible values are `phase_start` or `automatic`. If `phase_start` then billing cycle anchor of the subscription is set to the start of the phase when entering the phase. If `automatic` then the billing cycle anchor is automatically modified as needed when entering the phase. For more information, see the billing cycle [documentation](https://docs.stripe.com/billing/subscriptions/billing-cycle).
        """
        billing_thresholds: Optional[BillingThresholds]
        """
        Define thresholds at which an invoice will be sent, and the subscription advanced to a new billing period
        """
        collection_method: Optional[
            Literal["charge_automatically", "send_invoice"]
        ]
        """
        Either `charge_automatically`, or `send_invoice`. When charging automatically, Stripe will attempt to pay the underlying subscription at the end of each billing cycle using the default source attached to the customer. When sending an invoice, Stripe will email your customer an invoice with payment instructions and mark the subscription as `active`.
        """
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        default_payment_method: Optional[ExpandableField["PaymentMethod"]]
        """
        ID of the default payment method for the subscription schedule. It must belong to the customer associated with the subscription schedule. If not set, invoices will use the default payment method in the customer's invoice settings.
        """
        default_tax_rates: Optional[List["TaxRate"]]
        """
        The default tax rates to apply to the subscription during this phase of the subscription schedule.
        """
        description: Optional[str]
        """
        Subscription description, meant to be displayable to the customer. Use this field to optionally store an explanation of the subscription for rendering in Stripe surfaces and certain local payment methods UIs.
        """
        discounts: List[Discount]
        """
        The stackable discounts that will be applied to the subscription on this phase. Subscription item discounts are applied before subscription discounts.
        """
        end_date: int
        """
        The end of this phase of the subscription schedule.
        """
        invoice_settings: Optional[InvoiceSettings]
        """
        The invoice settings applicable during this phase.
        """
        items: List[Item]
        """
        Subscription items to configure the subscription to during this phase of the subscription schedule.
        """
        metadata: Optional[Dict[str, str]]
        """
        Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to a phase. Metadata on a schedule's phase will update the underlying subscription's `metadata` when the phase is entered. Updating the underlying subscription's `metadata` directly will not affect the current phase's `metadata`.
        """
        on_behalf_of: Optional[ExpandableField["Account"]]
        """
        The account (if any) the charge was made on behalf of for charges associated with the schedule's subscription. See the Connect documentation for details.
        """
        proration_behavior: Literal[
            "always_invoice", "create_prorations", "none"
        ]
        """
        When transitioning phases, controls how prorations are handled (if any). Possible values are `create_prorations`, `none`, and `always_invoice`.
        """
        start_date: int
        """
        The start of this phase of the subscription schedule.
        """
        transfer_data: Optional[TransferData]
        """
        The account (if any) the associated subscription's payments will be attributed to for tax reporting, and where funds from each payment will be transferred to for each of the subscription's invoices.
        """
        trial_end: Optional[int]
        """
        When the trial ends within the phase.
        """
        _inner_class_types = {
            "add_invoice_items": AddInvoiceItem,
            "automatic_tax": AutomaticTax,
            "billing_thresholds": BillingThresholds,
            "discounts": Discount,
            "invoice_settings": InvoiceSettings,
            "items": Item,
            "transfer_data": TransferData,
        }

    application: Optional[ExpandableField["Application"]]
    """
    ID of the Connect Application that created the schedule.
    """
    billing_mode: BillingMode
    """
    The billing mode of the subscription.
    """
    canceled_at: Optional[int]
    """
    Time at which the subscription schedule was canceled. Measured in seconds since the Unix epoch.
    """
    completed_at: Optional[int]
    """
    Time at which the subscription schedule was completed. Measured in seconds since the Unix epoch.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    current_phase: Optional[CurrentPhase]
    """
    Object representing the start and end dates for the current phase of the subscription schedule, if it is `active`.
    """
    customer: ExpandableField["Customer"]
    """
    ID of the customer who owns the subscription schedule.
    """
    customer_account: Optional[str]
    """
    ID of the account who owns the subscription schedule.
    """
    default_settings: DefaultSettings
    end_behavior: Literal["cancel", "none", "release", "renew"]
    """
    Behavior of the subscription schedule and underlying subscription when it ends. Possible values are `release` or `cancel` with the default being `release`. `release` will end the subscription schedule and keep the underlying subscription running. `cancel` will end the subscription schedule and cancel the underlying subscription.
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
    object: Literal["subscription_schedule"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    phases: List[Phase]
    """
    Configuration for the subscription schedule's phases.
    """
    released_at: Optional[int]
    """
    Time at which the subscription schedule was released. Measured in seconds since the Unix epoch.
    """
    released_subscription: Optional[str]
    """
    ID of the subscription once managed by the subscription schedule (if it is released).
    """
    status: Literal[
        "active", "canceled", "completed", "not_started", "released"
    ]
    """
    The present status of the subscription schedule. Possible values are `not_started`, `active`, `completed`, `released`, and `canceled`. You can read more about the different states in our [behavior guide](https://docs.stripe.com/billing/subscriptions/subscription-schedules).
    """
    subscription: Optional[ExpandableField["Subscription"]]
    """
    ID of the subscription managed by the subscription schedule.
    """
    test_clock: Optional[ExpandableField["TestClock"]]
    """
    ID of the test clock this subscription schedule belongs to.
    """

    @classmethod
    def _cls_cancel(
        cls,
        schedule: str,
        **params: Unpack["SubscriptionScheduleCancelParams"],
    ) -> "SubscriptionSchedule":
        """
        Cancels a subscription schedule and its associated subscription immediately (if the subscription schedule has an active subscription). A subscription schedule can only be canceled if its status is not_started or active.
        """
        return cast(
            "SubscriptionSchedule",
            cls._static_request(
                "post",
                "/v1/subscription_schedules/{schedule}/cancel".format(
                    schedule=sanitize_id(schedule)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def cancel(
        schedule: str, **params: Unpack["SubscriptionScheduleCancelParams"]
    ) -> "SubscriptionSchedule":
        """
        Cancels a subscription schedule and its associated subscription immediately (if the subscription schedule has an active subscription). A subscription schedule can only be canceled if its status is not_started or active.
        """
        ...

    @overload
    def cancel(
        self, **params: Unpack["SubscriptionScheduleCancelParams"]
    ) -> "SubscriptionSchedule":
        """
        Cancels a subscription schedule and its associated subscription immediately (if the subscription schedule has an active subscription). A subscription schedule can only be canceled if its status is not_started or active.
        """
        ...

    @class_method_variant("_cls_cancel")
    def cancel(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SubscriptionScheduleCancelParams"]
    ) -> "SubscriptionSchedule":
        """
        Cancels a subscription schedule and its associated subscription immediately (if the subscription schedule has an active subscription). A subscription schedule can only be canceled if its status is not_started or active.
        """
        return cast(
            "SubscriptionSchedule",
            self._request(
                "post",
                "/v1/subscription_schedules/{schedule}/cancel".format(
                    schedule=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_cancel_async(
        cls,
        schedule: str,
        **params: Unpack["SubscriptionScheduleCancelParams"],
    ) -> "SubscriptionSchedule":
        """
        Cancels a subscription schedule and its associated subscription immediately (if the subscription schedule has an active subscription). A subscription schedule can only be canceled if its status is not_started or active.
        """
        return cast(
            "SubscriptionSchedule",
            await cls._static_request_async(
                "post",
                "/v1/subscription_schedules/{schedule}/cancel".format(
                    schedule=sanitize_id(schedule)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def cancel_async(
        schedule: str, **params: Unpack["SubscriptionScheduleCancelParams"]
    ) -> "SubscriptionSchedule":
        """
        Cancels a subscription schedule and its associated subscription immediately (if the subscription schedule has an active subscription). A subscription schedule can only be canceled if its status is not_started or active.
        """
        ...

    @overload
    async def cancel_async(
        self, **params: Unpack["SubscriptionScheduleCancelParams"]
    ) -> "SubscriptionSchedule":
        """
        Cancels a subscription schedule and its associated subscription immediately (if the subscription schedule has an active subscription). A subscription schedule can only be canceled if its status is not_started or active.
        """
        ...

    @class_method_variant("_cls_cancel_async")
    async def cancel_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SubscriptionScheduleCancelParams"]
    ) -> "SubscriptionSchedule":
        """
        Cancels a subscription schedule and its associated subscription immediately (if the subscription schedule has an active subscription). A subscription schedule can only be canceled if its status is not_started or active.
        """
        return cast(
            "SubscriptionSchedule",
            await self._request_async(
                "post",
                "/v1/subscription_schedules/{schedule}/cancel".format(
                    schedule=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(
        cls, **params: Unpack["SubscriptionScheduleCreateParams"]
    ) -> "SubscriptionSchedule":
        """
        Creates a new subscription schedule object. Each customer can have up to 500 active or scheduled subscriptions.
        """
        return cast(
            "SubscriptionSchedule",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["SubscriptionScheduleCreateParams"]
    ) -> "SubscriptionSchedule":
        """
        Creates a new subscription schedule object. Each customer can have up to 500 active or scheduled subscriptions.
        """
        return cast(
            "SubscriptionSchedule",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["SubscriptionScheduleListParams"]
    ) -> ListObject["SubscriptionSchedule"]:
        """
        Retrieves the list of your subscription schedules.
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
        cls, **params: Unpack["SubscriptionScheduleListParams"]
    ) -> ListObject["SubscriptionSchedule"]:
        """
        Retrieves the list of your subscription schedules.
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
        cls, id: str, **params: Unpack["SubscriptionScheduleModifyParams"]
    ) -> "SubscriptionSchedule":
        """
        Updates an existing subscription schedule.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "SubscriptionSchedule",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["SubscriptionScheduleModifyParams"]
    ) -> "SubscriptionSchedule":
        """
        Updates an existing subscription schedule.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "SubscriptionSchedule",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def _cls_release(
        cls,
        schedule: str,
        **params: Unpack["SubscriptionScheduleReleaseParams"],
    ) -> "SubscriptionSchedule":
        """
        Releases the subscription schedule immediately, which will stop scheduling of its phases, but leave any existing subscription in place. A schedule can only be released if its status is not_started or active. If the subscription schedule is currently associated with a subscription, releasing it will remove its subscription property and set the subscription's ID to the released_subscription property.
        """
        return cast(
            "SubscriptionSchedule",
            cls._static_request(
                "post",
                "/v1/subscription_schedules/{schedule}/release".format(
                    schedule=sanitize_id(schedule)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def release(
        schedule: str, **params: Unpack["SubscriptionScheduleReleaseParams"]
    ) -> "SubscriptionSchedule":
        """
        Releases the subscription schedule immediately, which will stop scheduling of its phases, but leave any existing subscription in place. A schedule can only be released if its status is not_started or active. If the subscription schedule is currently associated with a subscription, releasing it will remove its subscription property and set the subscription's ID to the released_subscription property.
        """
        ...

    @overload
    def release(
        self, **params: Unpack["SubscriptionScheduleReleaseParams"]
    ) -> "SubscriptionSchedule":
        """
        Releases the subscription schedule immediately, which will stop scheduling of its phases, but leave any existing subscription in place. A schedule can only be released if its status is not_started or active. If the subscription schedule is currently associated with a subscription, releasing it will remove its subscription property and set the subscription's ID to the released_subscription property.
        """
        ...

    @class_method_variant("_cls_release")
    def release(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SubscriptionScheduleReleaseParams"]
    ) -> "SubscriptionSchedule":
        """
        Releases the subscription schedule immediately, which will stop scheduling of its phases, but leave any existing subscription in place. A schedule can only be released if its status is not_started or active. If the subscription schedule is currently associated with a subscription, releasing it will remove its subscription property and set the subscription's ID to the released_subscription property.
        """
        return cast(
            "SubscriptionSchedule",
            self._request(
                "post",
                "/v1/subscription_schedules/{schedule}/release".format(
                    schedule=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_release_async(
        cls,
        schedule: str,
        **params: Unpack["SubscriptionScheduleReleaseParams"],
    ) -> "SubscriptionSchedule":
        """
        Releases the subscription schedule immediately, which will stop scheduling of its phases, but leave any existing subscription in place. A schedule can only be released if its status is not_started or active. If the subscription schedule is currently associated with a subscription, releasing it will remove its subscription property and set the subscription's ID to the released_subscription property.
        """
        return cast(
            "SubscriptionSchedule",
            await cls._static_request_async(
                "post",
                "/v1/subscription_schedules/{schedule}/release".format(
                    schedule=sanitize_id(schedule)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def release_async(
        schedule: str, **params: Unpack["SubscriptionScheduleReleaseParams"]
    ) -> "SubscriptionSchedule":
        """
        Releases the subscription schedule immediately, which will stop scheduling of its phases, but leave any existing subscription in place. A schedule can only be released if its status is not_started or active. If the subscription schedule is currently associated with a subscription, releasing it will remove its subscription property and set the subscription's ID to the released_subscription property.
        """
        ...

    @overload
    async def release_async(
        self, **params: Unpack["SubscriptionScheduleReleaseParams"]
    ) -> "SubscriptionSchedule":
        """
        Releases the subscription schedule immediately, which will stop scheduling of its phases, but leave any existing subscription in place. A schedule can only be released if its status is not_started or active. If the subscription schedule is currently associated with a subscription, releasing it will remove its subscription property and set the subscription's ID to the released_subscription property.
        """
        ...

    @class_method_variant("_cls_release_async")
    async def release_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SubscriptionScheduleReleaseParams"]
    ) -> "SubscriptionSchedule":
        """
        Releases the subscription schedule immediately, which will stop scheduling of its phases, but leave any existing subscription in place. A schedule can only be released if its status is not_started or active. If the subscription schedule is currently associated with a subscription, releasing it will remove its subscription property and set the subscription's ID to the released_subscription property.
        """
        return cast(
            "SubscriptionSchedule",
            await self._request_async(
                "post",
                "/v1/subscription_schedules/{schedule}/release".format(
                    schedule=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["SubscriptionScheduleRetrieveParams"]
    ) -> "SubscriptionSchedule":
        """
        Retrieves the details of an existing subscription schedule. You only need to supply the unique subscription schedule identifier that was returned upon subscription schedule creation.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["SubscriptionScheduleRetrieveParams"]
    ) -> "SubscriptionSchedule":
        """
        Retrieves the details of an existing subscription schedule. You only need to supply the unique subscription schedule identifier that was returned upon subscription schedule creation.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "billing_mode": BillingMode,
        "current_phase": CurrentPhase,
        "default_settings": DefaultSettings,
        "phases": Phase,
    }
