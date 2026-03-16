# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._nested_resource_class_methods import nested_resource_class_methods
from stripe._search_result_object import SearchResultObject
from stripe._searchable_api_resource import SearchableAPIResource
from stripe._stripe_object import StripeObject
from stripe._test_helpers import APIResourceTestHelpers
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import (
    AsyncIterator,
    ClassVar,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
    cast,
    overload,
)
from typing_extensions import Literal, Type, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._account import Account
    from stripe._bank_account import BankAccount
    from stripe._card import Card
    from stripe._cash_balance import CashBalance
    from stripe._customer_balance_transaction import CustomerBalanceTransaction
    from stripe._customer_cash_balance_transaction import (
        CustomerCashBalanceTransaction,
    )
    from stripe._discount import Discount
    from stripe._funding_instructions import FundingInstructions
    from stripe._payment_method import PaymentMethod
    from stripe._source import Source
    from stripe._subscription import Subscription
    from stripe._tax_id import TaxId
    from stripe.params._customer_create_balance_transaction_params import (
        CustomerCreateBalanceTransactionParams,
    )
    from stripe.params._customer_create_funding_instructions_params import (
        CustomerCreateFundingInstructionsParams,
    )
    from stripe.params._customer_create_params import CustomerCreateParams
    from stripe.params._customer_create_source_params import (
        CustomerCreateSourceParams,
    )
    from stripe.params._customer_create_tax_id_params import (
        CustomerCreateTaxIdParams,
    )
    from stripe.params._customer_delete_discount_params import (
        CustomerDeleteDiscountParams,
    )
    from stripe.params._customer_delete_params import CustomerDeleteParams
    from stripe.params._customer_delete_source_params import (
        CustomerDeleteSourceParams,
    )
    from stripe.params._customer_delete_tax_id_params import (
        CustomerDeleteTaxIdParams,
    )
    from stripe.params._customer_fund_cash_balance_params import (
        CustomerFundCashBalanceParams,
    )
    from stripe.params._customer_list_balance_transactions_params import (
        CustomerListBalanceTransactionsParams,
    )
    from stripe.params._customer_list_cash_balance_transactions_params import (
        CustomerListCashBalanceTransactionsParams,
    )
    from stripe.params._customer_list_params import CustomerListParams
    from stripe.params._customer_list_payment_methods_params import (
        CustomerListPaymentMethodsParams,
    )
    from stripe.params._customer_list_sources_params import (
        CustomerListSourcesParams,
    )
    from stripe.params._customer_list_tax_ids_params import (
        CustomerListTaxIdsParams,
    )
    from stripe.params._customer_modify_balance_transaction_params import (
        CustomerModifyBalanceTransactionParams,
    )
    from stripe.params._customer_modify_cash_balance_params import (
        CustomerModifyCashBalanceParams,
    )
    from stripe.params._customer_modify_params import CustomerModifyParams
    from stripe.params._customer_modify_source_params import (
        CustomerModifySourceParams,
    )
    from stripe.params._customer_retrieve_balance_transaction_params import (
        CustomerRetrieveBalanceTransactionParams,
    )
    from stripe.params._customer_retrieve_cash_balance_params import (
        CustomerRetrieveCashBalanceParams,
    )
    from stripe.params._customer_retrieve_cash_balance_transaction_params import (
        CustomerRetrieveCashBalanceTransactionParams,
    )
    from stripe.params._customer_retrieve_params import CustomerRetrieveParams
    from stripe.params._customer_retrieve_payment_method_params import (
        CustomerRetrievePaymentMethodParams,
    )
    from stripe.params._customer_retrieve_source_params import (
        CustomerRetrieveSourceParams,
    )
    from stripe.params._customer_retrieve_tax_id_params import (
        CustomerRetrieveTaxIdParams,
    )
    from stripe.params._customer_search_params import CustomerSearchParams
    from stripe.test_helpers._test_clock import TestClock


@nested_resource_class_methods("balance_transaction")
@nested_resource_class_methods("cash_balance_transaction")
@nested_resource_class_methods("source")
@nested_resource_class_methods("tax_id")
class Customer(
    CreateableAPIResource["Customer"],
    DeletableAPIResource["Customer"],
    ListableAPIResource["Customer"],
    SearchableAPIResource["Customer"],
    UpdateableAPIResource["Customer"],
):
    """
    This object represents a customer of your business. Use it to [create recurring charges](https://docs.stripe.com/invoicing/customer), [save payment](https://docs.stripe.com/payments/save-during-payment) and contact information,
    and track payments that belong to the same customer.
    """

    OBJECT_NAME: ClassVar[Literal["customer"]] = "customer"

    class Address(StripeObject):
        city: Optional[str]
        """
        City, district, suburb, town, or village.
        """
        country: Optional[str]
        """
        Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
        """
        line1: Optional[str]
        """
        Address line 1, such as the street, PO Box, or company name.
        """
        line2: Optional[str]
        """
        Address line 2, such as the apartment, suite, unit, or building.
        """
        postal_code: Optional[str]
        """
        ZIP or postal code.
        """
        state: Optional[str]
        """
        State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
        """

    class InvoiceSettings(StripeObject):
        class CustomField(StripeObject):
            name: str
            """
            The name of the custom field.
            """
            value: str
            """
            The value of the custom field.
            """

        class RenderingOptions(StripeObject):
            amount_tax_display: Optional[str]
            """
            How line-item prices and amounts will be displayed with respect to tax on invoice PDFs.
            """
            template: Optional[str]
            """
            ID of the invoice rendering template to be used for this customer's invoices. If set, the template will be used on all invoices for this customer unless a template is set directly on the invoice.
            """

        custom_fields: Optional[List[CustomField]]
        """
        Default custom fields to be displayed on invoices for this customer.
        """
        default_payment_method: Optional[ExpandableField["PaymentMethod"]]
        """
        ID of a payment method that's attached to the customer, to be used as the customer's default payment method for subscriptions and invoices.
        """
        footer: Optional[str]
        """
        Default footer to be displayed on invoices for this customer.
        """
        rendering_options: Optional[RenderingOptions]
        """
        Default options for invoice PDF rendering for this customer.
        """
        _inner_class_types = {
            "custom_fields": CustomField,
            "rendering_options": RenderingOptions,
        }

    class Shipping(StripeObject):
        class Address(StripeObject):
            city: Optional[str]
            """
            City, district, suburb, town, or village.
            """
            country: Optional[str]
            """
            Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
            """
            line1: Optional[str]
            """
            Address line 1, such as the street, PO Box, or company name.
            """
            line2: Optional[str]
            """
            Address line 2, such as the apartment, suite, unit, or building.
            """
            postal_code: Optional[str]
            """
            ZIP or postal code.
            """
            state: Optional[str]
            """
            State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
            """

        address: Optional[Address]
        carrier: Optional[str]
        """
        The delivery service that shipped a physical product, such as Fedex, UPS, USPS, etc.
        """
        name: Optional[str]
        """
        Recipient name.
        """
        phone: Optional[str]
        """
        Recipient phone (including extension).
        """
        tracking_number: Optional[str]
        """
        The tracking number for a physical product, obtained from the delivery service. If multiple tracking numbers were generated for this purchase, please separate them with commas.
        """
        _inner_class_types = {"address": Address}

    class Tax(StripeObject):
        class Location(StripeObject):
            country: str
            """
            The identified tax country of the customer.
            """
            source: Literal[
                "billing_address",
                "ip_address",
                "payment_method",
                "shipping_destination",
            ]
            """
            The data source used to infer the customer's location.
            """
            state: Optional[str]
            """
            The identified tax state, county, province, or region of the customer.
            """

        automatic_tax: Literal[
            "failed", "not_collecting", "supported", "unrecognized_location"
        ]
        """
        Surfaces if automatic tax computation is possible given the current customer location information.
        """
        ip_address: Optional[str]
        """
        A recent IP address of the customer used for tax reporting and tax location inference.
        """
        location: Optional[Location]
        """
        The identified tax location of the customer.
        """
        provider: Literal["anrok", "avalara", "sphere", "stripe"]
        """
        The tax calculation provider used for location resolution. Defaults to `stripe` when not using a [third-party provider](https://docs.stripe.com/tax/third-party-apps).
        """
        _inner_class_types = {"location": Location}

    address: Optional[Address]
    """
    The customer's address.
    """
    balance: Optional[int]
    """
    The current balance, if any, that's stored on the customer in their default currency. If negative, the customer has credit to apply to their next invoice. If positive, the customer has an amount owed that's added to their next invoice. The balance only considers amounts that Stripe hasn't successfully applied to any invoice. It doesn't reflect unpaid invoices. This balance is only taken into account after invoices finalize. For multi-currency balances, see [invoice_credit_balance](https://docs.stripe.com/api/customers/object#customer_object-invoice_credit_balance).
    """
    business_name: Optional[str]
    """
    The customer's business name.
    """
    cash_balance: Optional["CashBalance"]
    """
    The current funds being held by Stripe on behalf of the customer. You can apply these funds towards payment intents when the source is "cash_balance". The `settings[reconciliation_mode]` field describes if these funds apply to these payment intents manually or automatically.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: Optional[str]
    """
    Three-letter [ISO code for the currency](https://stripe.com/docs/currencies) the customer can be charged in for recurring billing purposes.
    """
    customer_account: Optional[str]
    """
    The ID of an Account representing a customer. You can use this ID with any v1 API that accepts a customer_account parameter.
    """
    default_source: Optional[
        ExpandableField[Union["Account", "BankAccount", "Card", "Source"]]
    ]
    """
    ID of the default payment source for the customer.

    If you use payment methods created through the PaymentMethods API, see the [invoice_settings.default_payment_method](https://docs.stripe.com/api/customers/object#customer_object-invoice_settings-default_payment_method) field instead.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    delinquent: Optional[bool]
    """
    Tracks the most recent state change on any invoice belonging to the customer. Paying an invoice or marking it uncollectible via the API will set this field to false. An automatic payment failure or passing the `invoice.due_date` will set this field to `true`.

    If an invoice becomes uncollectible by [dunning](https://docs.stripe.com/billing/automatic-collection), `delinquent` doesn't reset to `false`.

    If you care whether the customer has paid their most recent subscription invoice, use `subscription.status` instead. Paying or marking uncollectible any customer invoice regardless of whether it is the latest invoice for a subscription will always set this field to `false`.
    """
    description: Optional[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    discount: Optional["Discount"]
    """
    Describes the current discount active on the customer, if there is one.
    """
    email: Optional[str]
    """
    The customer's email address.
    """
    id: str
    """
    Unique identifier for the object.
    """
    individual_name: Optional[str]
    """
    The customer's individual name.
    """
    invoice_credit_balance: Optional[Dict[str, int]]
    """
    The current multi-currency balances, if any, that's stored on the customer. If positive in a currency, the customer has a credit to apply to their next invoice denominated in that currency. If negative, the customer has an amount owed that's added to their next invoice denominated in that currency. These balances don't apply to unpaid invoices. They solely track amounts that Stripe hasn't successfully applied to any invoice. Stripe only applies a balance in a specific currency to an invoice after that invoice (which is in the same currency) finalizes.
    """
    invoice_prefix: Optional[str]
    """
    The prefix for the customer used to generate unique invoice numbers.
    """
    invoice_settings: Optional[InvoiceSettings]
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    name: Optional[str]
    """
    The customer's full name or business name.
    """
    next_invoice_sequence: Optional[int]
    """
    The suffix of the customer's next invoice number (for example, 0001). When the account uses account level sequencing, this parameter is ignored in API requests and the field omitted in API responses.
    """
    object: Literal["customer"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    phone: Optional[str]
    """
    The customer's phone number.
    """
    preferred_locales: Optional[List[str]]
    """
    The customer's preferred locales (languages), ordered by preference.
    """
    shipping: Optional[Shipping]
    """
    Mailing and shipping address for the customer. Appears on invoices emailed to this customer.
    """
    sources: Optional[
        ListObject[Union["Account", "BankAccount", "Card", "Source"]]
    ]
    """
    The customer's payment sources, if any.
    """
    subscriptions: Optional[ListObject["Subscription"]]
    """
    The customer's current subscriptions, if any.
    """
    tax: Optional[Tax]
    tax_exempt: Optional[Literal["exempt", "none", "reverse"]]
    """
    Describes the customer's tax exemption status, which is `none`, `exempt`, or `reverse`. When set to `reverse`, invoice and receipt PDFs include the following text: **"Reverse charge"**.
    """
    tax_ids: Optional[ListObject["TaxId"]]
    """
    The customer's tax IDs.
    """
    test_clock: Optional[ExpandableField["TestClock"]]
    """
    ID of the test clock that this customer belongs to.
    """

    @classmethod
    def create(cls, **params: Unpack["CustomerCreateParams"]) -> "Customer":
        """
        Creates a new customer object.
        """
        return cast(
            "Customer",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["CustomerCreateParams"]
    ) -> "Customer":
        """
        Creates a new customer object.
        """
        return cast(
            "Customer",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_create_funding_instructions(
        cls,
        customer: str,
        **params: Unpack["CustomerCreateFundingInstructionsParams"],
    ) -> "FundingInstructions":
        """
        Retrieve funding instructions for a customer cash balance. If funding instructions do not yet exist for the customer, new
        funding instructions will be created. If funding instructions have already been created for a given customer, the same
        funding instructions will be retrieved. In other words, we will return the same funding instructions each time.
        """
        return cast(
            "FundingInstructions",
            cls._static_request(
                "post",
                "/v1/customers/{customer}/funding_instructions".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def create_funding_instructions(
        customer: str,
        **params: Unpack["CustomerCreateFundingInstructionsParams"],
    ) -> "FundingInstructions":
        """
        Retrieve funding instructions for a customer cash balance. If funding instructions do not yet exist for the customer, new
        funding instructions will be created. If funding instructions have already been created for a given customer, the same
        funding instructions will be retrieved. In other words, we will return the same funding instructions each time.
        """
        ...

    @overload
    def create_funding_instructions(
        self, **params: Unpack["CustomerCreateFundingInstructionsParams"]
    ) -> "FundingInstructions":
        """
        Retrieve funding instructions for a customer cash balance. If funding instructions do not yet exist for the customer, new
        funding instructions will be created. If funding instructions have already been created for a given customer, the same
        funding instructions will be retrieved. In other words, we will return the same funding instructions each time.
        """
        ...

    @class_method_variant("_cls_create_funding_instructions")
    def create_funding_instructions(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["CustomerCreateFundingInstructionsParams"]
    ) -> "FundingInstructions":
        """
        Retrieve funding instructions for a customer cash balance. If funding instructions do not yet exist for the customer, new
        funding instructions will be created. If funding instructions have already been created for a given customer, the same
        funding instructions will be retrieved. In other words, we will return the same funding instructions each time.
        """
        return cast(
            "FundingInstructions",
            self._request(
                "post",
                "/v1/customers/{customer}/funding_instructions".format(
                    customer=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_create_funding_instructions_async(
        cls,
        customer: str,
        **params: Unpack["CustomerCreateFundingInstructionsParams"],
    ) -> "FundingInstructions":
        """
        Retrieve funding instructions for a customer cash balance. If funding instructions do not yet exist for the customer, new
        funding instructions will be created. If funding instructions have already been created for a given customer, the same
        funding instructions will be retrieved. In other words, we will return the same funding instructions each time.
        """
        return cast(
            "FundingInstructions",
            await cls._static_request_async(
                "post",
                "/v1/customers/{customer}/funding_instructions".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def create_funding_instructions_async(
        customer: str,
        **params: Unpack["CustomerCreateFundingInstructionsParams"],
    ) -> "FundingInstructions":
        """
        Retrieve funding instructions for a customer cash balance. If funding instructions do not yet exist for the customer, new
        funding instructions will be created. If funding instructions have already been created for a given customer, the same
        funding instructions will be retrieved. In other words, we will return the same funding instructions each time.
        """
        ...

    @overload
    async def create_funding_instructions_async(
        self, **params: Unpack["CustomerCreateFundingInstructionsParams"]
    ) -> "FundingInstructions":
        """
        Retrieve funding instructions for a customer cash balance. If funding instructions do not yet exist for the customer, new
        funding instructions will be created. If funding instructions have already been created for a given customer, the same
        funding instructions will be retrieved. In other words, we will return the same funding instructions each time.
        """
        ...

    @class_method_variant("_cls_create_funding_instructions_async")
    async def create_funding_instructions_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["CustomerCreateFundingInstructionsParams"]
    ) -> "FundingInstructions":
        """
        Retrieve funding instructions for a customer cash balance. If funding instructions do not yet exist for the customer, new
        funding instructions will be created. If funding instructions have already been created for a given customer, the same
        funding instructions will be retrieved. In other words, we will return the same funding instructions each time.
        """
        return cast(
            "FundingInstructions",
            await self._request_async(
                "post",
                "/v1/customers/{customer}/funding_instructions".format(
                    customer=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["CustomerDeleteParams"]
    ) -> "Customer":
        """
        Permanently deletes a customer. It cannot be undone. Also immediately cancels any active subscriptions on the customer.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Customer",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(
        sid: str, **params: Unpack["CustomerDeleteParams"]
    ) -> "Customer":
        """
        Permanently deletes a customer. It cannot be undone. Also immediately cancels any active subscriptions on the customer.
        """
        ...

    @overload
    def delete(self, **params: Unpack["CustomerDeleteParams"]) -> "Customer":
        """
        Permanently deletes a customer. It cannot be undone. Also immediately cancels any active subscriptions on the customer.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["CustomerDeleteParams"]
    ) -> "Customer":
        """
        Permanently deletes a customer. It cannot be undone. Also immediately cancels any active subscriptions on the customer.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["CustomerDeleteParams"]
    ) -> "Customer":
        """
        Permanently deletes a customer. It cannot be undone. Also immediately cancels any active subscriptions on the customer.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Customer",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["CustomerDeleteParams"]
    ) -> "Customer":
        """
        Permanently deletes a customer. It cannot be undone. Also immediately cancels any active subscriptions on the customer.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["CustomerDeleteParams"]
    ) -> "Customer":
        """
        Permanently deletes a customer. It cannot be undone. Also immediately cancels any active subscriptions on the customer.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["CustomerDeleteParams"]
    ) -> "Customer":
        """
        Permanently deletes a customer. It cannot be undone. Also immediately cancels any active subscriptions on the customer.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def _cls_delete_discount(
        cls, customer: str, **params: Unpack["CustomerDeleteDiscountParams"]
    ) -> "Discount":
        """
        Removes the currently applied discount on a customer.
        """
        return cast(
            "Discount",
            cls._static_request(
                "delete",
                "/v1/customers/{customer}/discount".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete_discount(
        customer: str, **params: Unpack["CustomerDeleteDiscountParams"]
    ) -> "Discount":
        """
        Removes the currently applied discount on a customer.
        """
        ...

    @overload
    def delete_discount(
        self, **params: Unpack["CustomerDeleteDiscountParams"]
    ) -> "Discount":
        """
        Removes the currently applied discount on a customer.
        """
        ...

    @class_method_variant("_cls_delete_discount")
    def delete_discount(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["CustomerDeleteDiscountParams"]
    ) -> "Discount":
        """
        Removes the currently applied discount on a customer.
        """
        return cast(
            "Discount",
            self._request(
                "delete",
                "/v1/customers/{customer}/discount".format(
                    customer=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_delete_discount_async(
        cls, customer: str, **params: Unpack["CustomerDeleteDiscountParams"]
    ) -> "Discount":
        """
        Removes the currently applied discount on a customer.
        """
        return cast(
            "Discount",
            await cls._static_request_async(
                "delete",
                "/v1/customers/{customer}/discount".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_discount_async(
        customer: str, **params: Unpack["CustomerDeleteDiscountParams"]
    ) -> "Discount":
        """
        Removes the currently applied discount on a customer.
        """
        ...

    @overload
    async def delete_discount_async(
        self, **params: Unpack["CustomerDeleteDiscountParams"]
    ) -> "Discount":
        """
        Removes the currently applied discount on a customer.
        """
        ...

    @class_method_variant("_cls_delete_discount_async")
    async def delete_discount_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["CustomerDeleteDiscountParams"]
    ) -> "Discount":
        """
        Removes the currently applied discount on a customer.
        """
        return cast(
            "Discount",
            await self._request_async(
                "delete",
                "/v1/customers/{customer}/discount".format(
                    customer=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["CustomerListParams"]
    ) -> ListObject["Customer"]:
        """
        Returns a list of your customers. The customers are returned sorted by creation date, with the most recent customers appearing first.
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
        cls, **params: Unpack["CustomerListParams"]
    ) -> ListObject["Customer"]:
        """
        Returns a list of your customers. The customers are returned sorted by creation date, with the most recent customers appearing first.
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
    def _cls_list_payment_methods(
        cls,
        customer: str,
        **params: Unpack["CustomerListPaymentMethodsParams"],
    ) -> ListObject["PaymentMethod"]:
        """
        Returns a list of PaymentMethods for a given Customer
        """
        return cast(
            ListObject["PaymentMethod"],
            cls._static_request(
                "get",
                "/v1/customers/{customer}/payment_methods".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def list_payment_methods(
        customer: str, **params: Unpack["CustomerListPaymentMethodsParams"]
    ) -> ListObject["PaymentMethod"]:
        """
        Returns a list of PaymentMethods for a given Customer
        """
        ...

    @overload
    def list_payment_methods(
        self, **params: Unpack["CustomerListPaymentMethodsParams"]
    ) -> ListObject["PaymentMethod"]:
        """
        Returns a list of PaymentMethods for a given Customer
        """
        ...

    @class_method_variant("_cls_list_payment_methods")
    def list_payment_methods(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["CustomerListPaymentMethodsParams"]
    ) -> ListObject["PaymentMethod"]:
        """
        Returns a list of PaymentMethods for a given Customer
        """
        return cast(
            ListObject["PaymentMethod"],
            self._request(
                "get",
                "/v1/customers/{customer}/payment_methods".format(
                    customer=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_list_payment_methods_async(
        cls,
        customer: str,
        **params: Unpack["CustomerListPaymentMethodsParams"],
    ) -> ListObject["PaymentMethod"]:
        """
        Returns a list of PaymentMethods for a given Customer
        """
        return cast(
            ListObject["PaymentMethod"],
            await cls._static_request_async(
                "get",
                "/v1/customers/{customer}/payment_methods".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def list_payment_methods_async(
        customer: str, **params: Unpack["CustomerListPaymentMethodsParams"]
    ) -> ListObject["PaymentMethod"]:
        """
        Returns a list of PaymentMethods for a given Customer
        """
        ...

    @overload
    async def list_payment_methods_async(
        self, **params: Unpack["CustomerListPaymentMethodsParams"]
    ) -> ListObject["PaymentMethod"]:
        """
        Returns a list of PaymentMethods for a given Customer
        """
        ...

    @class_method_variant("_cls_list_payment_methods_async")
    async def list_payment_methods_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["CustomerListPaymentMethodsParams"]
    ) -> ListObject["PaymentMethod"]:
        """
        Returns a list of PaymentMethods for a given Customer
        """
        return cast(
            ListObject["PaymentMethod"],
            await self._request_async(
                "get",
                "/v1/customers/{customer}/payment_methods".format(
                    customer=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def modify(
        cls, id: str, **params: Unpack["CustomerModifyParams"]
    ) -> "Customer":
        """
        Updates the specified customer by setting the values of the parameters passed. Any parameters not provided are left unchanged. For example, if you pass the source parameter, that becomes the customer's active source (such as a card) to be used for all charges in the future. When you update a customer to a new valid card source by passing the source parameter: for each of the customer's current subscriptions, if the subscription bills automatically and is in the past_due state, then the latest open invoice for the subscription with automatic collection enabled is retried. This retry doesn't count as an automatic retry, and doesn't affect the next regularly scheduled payment for the invoice. Changing the default_source for a customer doesn't trigger this behavior.

        This request accepts mostly the same arguments as the customer creation call.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Customer",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["CustomerModifyParams"]
    ) -> "Customer":
        """
        Updates the specified customer by setting the values of the parameters passed. Any parameters not provided are left unchanged. For example, if you pass the source parameter, that becomes the customer's active source (such as a card) to be used for all charges in the future. When you update a customer to a new valid card source by passing the source parameter: for each of the customer's current subscriptions, if the subscription bills automatically and is in the past_due state, then the latest open invoice for the subscription with automatic collection enabled is retried. This retry doesn't count as an automatic retry, and doesn't affect the next regularly scheduled payment for the invoice. Changing the default_source for a customer doesn't trigger this behavior.

        This request accepts mostly the same arguments as the customer creation call.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Customer",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["CustomerRetrieveParams"]
    ) -> "Customer":
        """
        Retrieves a Customer object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["CustomerRetrieveParams"]
    ) -> "Customer":
        """
        Retrieves a Customer object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def _cls_retrieve_payment_method(
        cls,
        customer: str,
        payment_method: str,
        **params: Unpack["CustomerRetrievePaymentMethodParams"],
    ) -> "PaymentMethod":
        """
        Retrieves a PaymentMethod object for a given Customer.
        """
        return cast(
            "PaymentMethod",
            cls._static_request(
                "get",
                "/v1/customers/{customer}/payment_methods/{payment_method}".format(
                    customer=sanitize_id(customer),
                    payment_method=sanitize_id(payment_method),
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def retrieve_payment_method(
        customer: str,
        payment_method: str,
        **params: Unpack["CustomerRetrievePaymentMethodParams"],
    ) -> "PaymentMethod":
        """
        Retrieves a PaymentMethod object for a given Customer.
        """
        ...

    @overload
    def retrieve_payment_method(
        self,
        payment_method: str,
        **params: Unpack["CustomerRetrievePaymentMethodParams"],
    ) -> "PaymentMethod":
        """
        Retrieves a PaymentMethod object for a given Customer.
        """
        ...

    @class_method_variant("_cls_retrieve_payment_method")
    def retrieve_payment_method(  # pyright: ignore[reportGeneralTypeIssues]
        self,
        payment_method: str,
        **params: Unpack["CustomerRetrievePaymentMethodParams"],
    ) -> "PaymentMethod":
        """
        Retrieves a PaymentMethod object for a given Customer.
        """
        return cast(
            "PaymentMethod",
            self._request(
                "get",
                "/v1/customers/{customer}/payment_methods/{payment_method}".format(
                    customer=sanitize_id(self.get("id")),
                    payment_method=sanitize_id(payment_method),
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_retrieve_payment_method_async(
        cls,
        customer: str,
        payment_method: str,
        **params: Unpack["CustomerRetrievePaymentMethodParams"],
    ) -> "PaymentMethod":
        """
        Retrieves a PaymentMethod object for a given Customer.
        """
        return cast(
            "PaymentMethod",
            await cls._static_request_async(
                "get",
                "/v1/customers/{customer}/payment_methods/{payment_method}".format(
                    customer=sanitize_id(customer),
                    payment_method=sanitize_id(payment_method),
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def retrieve_payment_method_async(
        customer: str,
        payment_method: str,
        **params: Unpack["CustomerRetrievePaymentMethodParams"],
    ) -> "PaymentMethod":
        """
        Retrieves a PaymentMethod object for a given Customer.
        """
        ...

    @overload
    async def retrieve_payment_method_async(
        self,
        payment_method: str,
        **params: Unpack["CustomerRetrievePaymentMethodParams"],
    ) -> "PaymentMethod":
        """
        Retrieves a PaymentMethod object for a given Customer.
        """
        ...

    @class_method_variant("_cls_retrieve_payment_method_async")
    async def retrieve_payment_method_async(  # pyright: ignore[reportGeneralTypeIssues]
        self,
        payment_method: str,
        **params: Unpack["CustomerRetrievePaymentMethodParams"],
    ) -> "PaymentMethod":
        """
        Retrieves a PaymentMethod object for a given Customer.
        """
        return cast(
            "PaymentMethod",
            await self._request_async(
                "get",
                "/v1/customers/{customer}/payment_methods/{payment_method}".format(
                    customer=sanitize_id(self.get("id")),
                    payment_method=sanitize_id(payment_method),
                ),
                params=params,
            ),
        )

    @classmethod
    def search(
        cls, *args, **kwargs: Unpack["CustomerSearchParams"]
    ) -> SearchResultObject["Customer"]:
        """
        Search for customers you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cls._search(search_url="/v1/customers/search", *args, **kwargs)

    @classmethod
    async def search_async(
        cls, *args, **kwargs: Unpack["CustomerSearchParams"]
    ) -> SearchResultObject["Customer"]:
        """
        Search for customers you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return await cls._search_async(
            search_url="/v1/customers/search", *args, **kwargs
        )

    @classmethod
    def search_auto_paging_iter(
        cls, *args, **kwargs: Unpack["CustomerSearchParams"]
    ) -> Iterator["Customer"]:
        return cls.search(*args, **kwargs).auto_paging_iter()

    @classmethod
    async def search_auto_paging_iter_async(
        cls, *args, **kwargs: Unpack["CustomerSearchParams"]
    ) -> AsyncIterator["Customer"]:
        return (await cls.search_async(*args, **kwargs)).auto_paging_iter()

    @classmethod
    def list_balance_transactions(
        cls,
        customer: str,
        **params: Unpack["CustomerListBalanceTransactionsParams"],
    ) -> ListObject["CustomerBalanceTransaction"]:
        """
        Returns a list of transactions that updated the customer's [balances](https://docs.stripe.com/docs/billing/customer/balance).
        """
        return cast(
            ListObject["CustomerBalanceTransaction"],
            cls._static_request(
                "get",
                "/v1/customers/{customer}/balance_transactions".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    async def list_balance_transactions_async(
        cls,
        customer: str,
        **params: Unpack["CustomerListBalanceTransactionsParams"],
    ) -> ListObject["CustomerBalanceTransaction"]:
        """
        Returns a list of transactions that updated the customer's [balances](https://docs.stripe.com/docs/billing/customer/balance).
        """
        return cast(
            ListObject["CustomerBalanceTransaction"],
            await cls._static_request_async(
                "get",
                "/v1/customers/{customer}/balance_transactions".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    def create_balance_transaction(
        cls,
        customer: str,
        **params: Unpack["CustomerCreateBalanceTransactionParams"],
    ) -> "CustomerBalanceTransaction":
        """
        Creates an immutable transaction that updates the customer's credit [balance](https://docs.stripe.com/docs/billing/customer/balance).
        """
        return cast(
            "CustomerBalanceTransaction",
            cls._static_request(
                "post",
                "/v1/customers/{customer}/balance_transactions".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    async def create_balance_transaction_async(
        cls,
        customer: str,
        **params: Unpack["CustomerCreateBalanceTransactionParams"],
    ) -> "CustomerBalanceTransaction":
        """
        Creates an immutable transaction that updates the customer's credit [balance](https://docs.stripe.com/docs/billing/customer/balance).
        """
        return cast(
            "CustomerBalanceTransaction",
            await cls._static_request_async(
                "post",
                "/v1/customers/{customer}/balance_transactions".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve_balance_transaction(
        cls,
        customer: str,
        transaction: str,
        **params: Unpack["CustomerRetrieveBalanceTransactionParams"],
    ) -> "CustomerBalanceTransaction":
        """
        Retrieves a specific customer balance transaction that updated the customer's [balances](https://docs.stripe.com/docs/billing/customer/balance).
        """
        return cast(
            "CustomerBalanceTransaction",
            cls._static_request(
                "get",
                "/v1/customers/{customer}/balance_transactions/{transaction}".format(
                    customer=sanitize_id(customer),
                    transaction=sanitize_id(transaction),
                ),
                params=params,
            ),
        )

    @classmethod
    async def retrieve_balance_transaction_async(
        cls,
        customer: str,
        transaction: str,
        **params: Unpack["CustomerRetrieveBalanceTransactionParams"],
    ) -> "CustomerBalanceTransaction":
        """
        Retrieves a specific customer balance transaction that updated the customer's [balances](https://docs.stripe.com/docs/billing/customer/balance).
        """
        return cast(
            "CustomerBalanceTransaction",
            await cls._static_request_async(
                "get",
                "/v1/customers/{customer}/balance_transactions/{transaction}".format(
                    customer=sanitize_id(customer),
                    transaction=sanitize_id(transaction),
                ),
                params=params,
            ),
        )

    @classmethod
    def modify_balance_transaction(
        cls,
        customer: str,
        transaction: str,
        **params: Unpack["CustomerModifyBalanceTransactionParams"],
    ) -> "CustomerBalanceTransaction":
        """
        Most credit balance transaction fields are immutable, but you may update its description and metadata.
        """
        return cast(
            "CustomerBalanceTransaction",
            cls._static_request(
                "post",
                "/v1/customers/{customer}/balance_transactions/{transaction}".format(
                    customer=sanitize_id(customer),
                    transaction=sanitize_id(transaction),
                ),
                params=params,
            ),
        )

    @classmethod
    async def modify_balance_transaction_async(
        cls,
        customer: str,
        transaction: str,
        **params: Unpack["CustomerModifyBalanceTransactionParams"],
    ) -> "CustomerBalanceTransaction":
        """
        Most credit balance transaction fields are immutable, but you may update its description and metadata.
        """
        return cast(
            "CustomerBalanceTransaction",
            await cls._static_request_async(
                "post",
                "/v1/customers/{customer}/balance_transactions/{transaction}".format(
                    customer=sanitize_id(customer),
                    transaction=sanitize_id(transaction),
                ),
                params=params,
            ),
        )

    @classmethod
    def list_cash_balance_transactions(
        cls,
        customer: str,
        **params: Unpack["CustomerListCashBalanceTransactionsParams"],
    ) -> ListObject["CustomerCashBalanceTransaction"]:
        """
        Returns a list of transactions that modified the customer's [cash balance](https://docs.stripe.com/docs/payments/customer-balance).
        """
        return cast(
            ListObject["CustomerCashBalanceTransaction"],
            cls._static_request(
                "get",
                "/v1/customers/{customer}/cash_balance_transactions".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    async def list_cash_balance_transactions_async(
        cls,
        customer: str,
        **params: Unpack["CustomerListCashBalanceTransactionsParams"],
    ) -> ListObject["CustomerCashBalanceTransaction"]:
        """
        Returns a list of transactions that modified the customer's [cash balance](https://docs.stripe.com/docs/payments/customer-balance).
        """
        return cast(
            ListObject["CustomerCashBalanceTransaction"],
            await cls._static_request_async(
                "get",
                "/v1/customers/{customer}/cash_balance_transactions".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve_cash_balance_transaction(
        cls,
        customer: str,
        transaction: str,
        **params: Unpack["CustomerRetrieveCashBalanceTransactionParams"],
    ) -> "CustomerCashBalanceTransaction":
        """
        Retrieves a specific cash balance transaction, which updated the customer's [cash balance](https://docs.stripe.com/docs/payments/customer-balance).
        """
        return cast(
            "CustomerCashBalanceTransaction",
            cls._static_request(
                "get",
                "/v1/customers/{customer}/cash_balance_transactions/{transaction}".format(
                    customer=sanitize_id(customer),
                    transaction=sanitize_id(transaction),
                ),
                params=params,
            ),
        )

    @classmethod
    async def retrieve_cash_balance_transaction_async(
        cls,
        customer: str,
        transaction: str,
        **params: Unpack["CustomerRetrieveCashBalanceTransactionParams"],
    ) -> "CustomerCashBalanceTransaction":
        """
        Retrieves a specific cash balance transaction, which updated the customer's [cash balance](https://docs.stripe.com/docs/payments/customer-balance).
        """
        return cast(
            "CustomerCashBalanceTransaction",
            await cls._static_request_async(
                "get",
                "/v1/customers/{customer}/cash_balance_transactions/{transaction}".format(
                    customer=sanitize_id(customer),
                    transaction=sanitize_id(transaction),
                ),
                params=params,
            ),
        )

    @classmethod
    def list_sources(
        cls, customer: str, **params: Unpack["CustomerListSourcesParams"]
    ) -> ListObject[Union["Account", "BankAccount", "Card", "Source"]]:
        """
        List sources for a specified customer.
        """
        return cast(
            ListObject[Union["Account", "BankAccount", "Card", "Source"]],
            cls._static_request(
                "get",
                "/v1/customers/{customer}/sources".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    async def list_sources_async(
        cls, customer: str, **params: Unpack["CustomerListSourcesParams"]
    ) -> ListObject[Union["Account", "BankAccount", "Card", "Source"]]:
        """
        List sources for a specified customer.
        """
        return cast(
            ListObject[Union["Account", "BankAccount", "Card", "Source"]],
            await cls._static_request_async(
                "get",
                "/v1/customers/{customer}/sources".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    def create_source(
        cls, customer: str, **params: Unpack["CustomerCreateSourceParams"]
    ) -> Union["Account", "BankAccount", "Card", "Source"]:
        """
        When you create a new credit card, you must specify a customer or recipient on which to create it.

        If the card's owner has no default card, then the new card will become the default.
        However, if the owner already has a default, then it will not change.
        To change the default, you should [update the customer](https://docs.stripe.com/docs/api#update_customer) to have a new default_source.
        """
        return cast(
            Union["Account", "BankAccount", "Card", "Source"],
            cls._static_request(
                "post",
                "/v1/customers/{customer}/sources".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    async def create_source_async(
        cls, customer: str, **params: Unpack["CustomerCreateSourceParams"]
    ) -> Union["Account", "BankAccount", "Card", "Source"]:
        """
        When you create a new credit card, you must specify a customer or recipient on which to create it.

        If the card's owner has no default card, then the new card will become the default.
        However, if the owner already has a default, then it will not change.
        To change the default, you should [update the customer](https://docs.stripe.com/docs/api#update_customer) to have a new default_source.
        """
        return cast(
            Union["Account", "BankAccount", "Card", "Source"],
            await cls._static_request_async(
                "post",
                "/v1/customers/{customer}/sources".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve_source(
        cls,
        customer: str,
        id: str,
        **params: Unpack["CustomerRetrieveSourceParams"],
    ) -> Union["Account", "BankAccount", "Card", "Source"]:
        """
        Retrieve a specified source for a given customer.
        """
        return cast(
            Union["Account", "BankAccount", "Card", "Source"],
            cls._static_request(
                "get",
                "/v1/customers/{customer}/sources/{id}".format(
                    customer=sanitize_id(customer), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    async def retrieve_source_async(
        cls,
        customer: str,
        id: str,
        **params: Unpack["CustomerRetrieveSourceParams"],
    ) -> Union["Account", "BankAccount", "Card", "Source"]:
        """
        Retrieve a specified source for a given customer.
        """
        return cast(
            Union["Account", "BankAccount", "Card", "Source"],
            await cls._static_request_async(
                "get",
                "/v1/customers/{customer}/sources/{id}".format(
                    customer=sanitize_id(customer), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    def modify_source(
        cls,
        customer: str,
        id: str,
        **params: Unpack["CustomerModifySourceParams"],
    ) -> Union["Account", "BankAccount", "Card", "Source"]:
        """
        Update a specified source for a given customer.
        """
        return cast(
            Union["Account", "BankAccount", "Card", "Source"],
            cls._static_request(
                "post",
                "/v1/customers/{customer}/sources/{id}".format(
                    customer=sanitize_id(customer), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    async def modify_source_async(
        cls,
        customer: str,
        id: str,
        **params: Unpack["CustomerModifySourceParams"],
    ) -> Union["Account", "BankAccount", "Card", "Source"]:
        """
        Update a specified source for a given customer.
        """
        return cast(
            Union["Account", "BankAccount", "Card", "Source"],
            await cls._static_request_async(
                "post",
                "/v1/customers/{customer}/sources/{id}".format(
                    customer=sanitize_id(customer), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    def delete_source(
        cls,
        customer: str,
        id: str,
        **params: Unpack["CustomerDeleteSourceParams"],
    ) -> Union["Account", "BankAccount", "Card", "Source"]:
        """
        Delete a specified source for a given customer.
        """
        return cast(
            Union["Account", "BankAccount", "Card", "Source"],
            cls._static_request(
                "delete",
                "/v1/customers/{customer}/sources/{id}".format(
                    customer=sanitize_id(customer), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    async def delete_source_async(
        cls,
        customer: str,
        id: str,
        **params: Unpack["CustomerDeleteSourceParams"],
    ) -> Union["Account", "BankAccount", "Card", "Source"]:
        """
        Delete a specified source for a given customer.
        """
        return cast(
            Union["Account", "BankAccount", "Card", "Source"],
            await cls._static_request_async(
                "delete",
                "/v1/customers/{customer}/sources/{id}".format(
                    customer=sanitize_id(customer), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    def create_tax_id(
        cls, customer: str, **params: Unpack["CustomerCreateTaxIdParams"]
    ) -> "TaxId":
        """
        Creates a new tax_id object for a customer.
        """
        return cast(
            "TaxId",
            cls._static_request(
                "post",
                "/v1/customers/{customer}/tax_ids".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    async def create_tax_id_async(
        cls, customer: str, **params: Unpack["CustomerCreateTaxIdParams"]
    ) -> "TaxId":
        """
        Creates a new tax_id object for a customer.
        """
        return cast(
            "TaxId",
            await cls._static_request_async(
                "post",
                "/v1/customers/{customer}/tax_ids".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve_tax_id(
        cls,
        customer: str,
        id: str,
        **params: Unpack["CustomerRetrieveTaxIdParams"],
    ) -> "TaxId":
        """
        Retrieves the tax_id object with the given identifier.
        """
        return cast(
            "TaxId",
            cls._static_request(
                "get",
                "/v1/customers/{customer}/tax_ids/{id}".format(
                    customer=sanitize_id(customer), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    async def retrieve_tax_id_async(
        cls,
        customer: str,
        id: str,
        **params: Unpack["CustomerRetrieveTaxIdParams"],
    ) -> "TaxId":
        """
        Retrieves the tax_id object with the given identifier.
        """
        return cast(
            "TaxId",
            await cls._static_request_async(
                "get",
                "/v1/customers/{customer}/tax_ids/{id}".format(
                    customer=sanitize_id(customer), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    def delete_tax_id(
        cls,
        customer: str,
        id: str,
        **params: Unpack["CustomerDeleteTaxIdParams"],
    ) -> "TaxId":
        """
        Deletes an existing tax_id object.
        """
        return cast(
            "TaxId",
            cls._static_request(
                "delete",
                "/v1/customers/{customer}/tax_ids/{id}".format(
                    customer=sanitize_id(customer), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    async def delete_tax_id_async(
        cls,
        customer: str,
        id: str,
        **params: Unpack["CustomerDeleteTaxIdParams"],
    ) -> "TaxId":
        """
        Deletes an existing tax_id object.
        """
        return cast(
            "TaxId",
            await cls._static_request_async(
                "delete",
                "/v1/customers/{customer}/tax_ids/{id}".format(
                    customer=sanitize_id(customer), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    def list_tax_ids(
        cls, customer: str, **params: Unpack["CustomerListTaxIdsParams"]
    ) -> ListObject["TaxId"]:
        """
        Returns a list of tax IDs for a customer.
        """
        return cast(
            ListObject["TaxId"],
            cls._static_request(
                "get",
                "/v1/customers/{customer}/tax_ids".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    async def list_tax_ids_async(
        cls, customer: str, **params: Unpack["CustomerListTaxIdsParams"]
    ) -> ListObject["TaxId"]:
        """
        Returns a list of tax IDs for a customer.
        """
        return cast(
            ListObject["TaxId"],
            await cls._static_request_async(
                "get",
                "/v1/customers/{customer}/tax_ids".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve_cash_balance(
        cls,
        customer: str,
        **params: Unpack["CustomerRetrieveCashBalanceParams"],
    ) -> "CashBalance":
        """
        Retrieves a customer's cash balance.
        """
        return cast(
            "CashBalance",
            cls._static_request(
                "get",
                "/v1/customers/{customer}/cash_balance".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    async def retrieve_cash_balance_async(
        cls,
        customer: str,
        **params: Unpack["CustomerRetrieveCashBalanceParams"],
    ) -> "CashBalance":
        """
        Retrieves a customer's cash balance.
        """
        return cast(
            "CashBalance",
            await cls._static_request_async(
                "get",
                "/v1/customers/{customer}/cash_balance".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    def modify_cash_balance(
        cls, customer: str, **params: Unpack["CustomerModifyCashBalanceParams"]
    ) -> "CashBalance":
        """
        Changes the settings on a customer's cash balance.
        """
        return cast(
            "CashBalance",
            cls._static_request(
                "post",
                "/v1/customers/{customer}/cash_balance".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    @classmethod
    async def modify_cash_balance_async(
        cls, customer: str, **params: Unpack["CustomerModifyCashBalanceParams"]
    ) -> "CashBalance":
        """
        Changes the settings on a customer's cash balance.
        """
        return cast(
            "CashBalance",
            await cls._static_request_async(
                "post",
                "/v1/customers/{customer}/cash_balance".format(
                    customer=sanitize_id(customer)
                ),
                params=params,
            ),
        )

    class TestHelpers(APIResourceTestHelpers["Customer"]):
        _resource_cls: Type["Customer"]

        @classmethod
        def _cls_fund_cash_balance(
            cls,
            customer: str,
            **params: Unpack["CustomerFundCashBalanceParams"],
        ) -> "CustomerCashBalanceTransaction":
            """
            Create an incoming testmode bank transfer
            """
            return cast(
                "CustomerCashBalanceTransaction",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/customers/{customer}/fund_cash_balance".format(
                        customer=sanitize_id(customer)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def fund_cash_balance(
            customer: str, **params: Unpack["CustomerFundCashBalanceParams"]
        ) -> "CustomerCashBalanceTransaction":
            """
            Create an incoming testmode bank transfer
            """
            ...

        @overload
        def fund_cash_balance(
            self, **params: Unpack["CustomerFundCashBalanceParams"]
        ) -> "CustomerCashBalanceTransaction":
            """
            Create an incoming testmode bank transfer
            """
            ...

        @class_method_variant("_cls_fund_cash_balance")
        def fund_cash_balance(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["CustomerFundCashBalanceParams"]
        ) -> "CustomerCashBalanceTransaction":
            """
            Create an incoming testmode bank transfer
            """
            return cast(
                "CustomerCashBalanceTransaction",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/customers/{customer}/fund_cash_balance".format(
                        customer=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_fund_cash_balance_async(
            cls,
            customer: str,
            **params: Unpack["CustomerFundCashBalanceParams"],
        ) -> "CustomerCashBalanceTransaction":
            """
            Create an incoming testmode bank transfer
            """
            return cast(
                "CustomerCashBalanceTransaction",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/customers/{customer}/fund_cash_balance".format(
                        customer=sanitize_id(customer)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def fund_cash_balance_async(
            customer: str, **params: Unpack["CustomerFundCashBalanceParams"]
        ) -> "CustomerCashBalanceTransaction":
            """
            Create an incoming testmode bank transfer
            """
            ...

        @overload
        async def fund_cash_balance_async(
            self, **params: Unpack["CustomerFundCashBalanceParams"]
        ) -> "CustomerCashBalanceTransaction":
            """
            Create an incoming testmode bank transfer
            """
            ...

        @class_method_variant("_cls_fund_cash_balance_async")
        async def fund_cash_balance_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["CustomerFundCashBalanceParams"]
        ) -> "CustomerCashBalanceTransaction":
            """
            Create an incoming testmode bank transfer
            """
            return cast(
                "CustomerCashBalanceTransaction",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/customers/{customer}/fund_cash_balance".format(
                        customer=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

    @property
    def test_helpers(self):
        return self.TestHelpers(self)

    _inner_class_types = {
        "address": Address,
        "invoice_settings": InvoiceSettings,
        "shipping": Shipping,
        "tax": Tax,
    }


Customer.TestHelpers._resource_cls = Customer
