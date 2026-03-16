# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._balance_transaction import BalanceTransaction
    from stripe._charge import Charge
    from stripe._file import File
    from stripe._payment_intent import PaymentIntent
    from stripe.params._dispute_close_params import DisputeCloseParams
    from stripe.params._dispute_list_params import DisputeListParams
    from stripe.params._dispute_modify_params import DisputeModifyParams
    from stripe.params._dispute_retrieve_params import DisputeRetrieveParams


class Dispute(
    ListableAPIResource["Dispute"], UpdateableAPIResource["Dispute"]
):
    """
    A dispute occurs when a customer questions your charge with their card issuer.
    When this happens, you have the opportunity to respond to the dispute with
    evidence that shows that the charge is legitimate.

    Related guide: [Disputes and fraud](https://docs.stripe.com/disputes)
    """

    OBJECT_NAME: ClassVar[Literal["dispute"]] = "dispute"

    class Evidence(StripeObject):
        class EnhancedEvidence(StripeObject):
            class VisaCompellingEvidence3(StripeObject):
                class DisputedTransaction(StripeObject):
                    class ShippingAddress(StripeObject):
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

                    customer_account_id: Optional[str]
                    """
                    User Account ID used to log into business platform. Must be recognizable by the user.
                    """
                    customer_device_fingerprint: Optional[str]
                    """
                    Unique identifier of the cardholder's device derived from a combination of at least two hardware and software attributes. Must be at least 20 characters.
                    """
                    customer_device_id: Optional[str]
                    """
                    Unique identifier of the cardholder's device such as a device serial number (e.g., International Mobile Equipment Identity [IMEI]). Must be at least 15 characters.
                    """
                    customer_email_address: Optional[str]
                    """
                    The email address of the customer.
                    """
                    customer_purchase_ip: Optional[str]
                    """
                    The IP address that the customer used when making the purchase.
                    """
                    merchandise_or_services: Optional[
                        Literal["merchandise", "services"]
                    ]
                    """
                    Categorization of disputed payment.
                    """
                    product_description: Optional[str]
                    """
                    A description of the product or service that was sold.
                    """
                    shipping_address: Optional[ShippingAddress]
                    """
                    The address to which a physical product was shipped. All fields are required for Visa Compelling Evidence 3.0 evidence submission.
                    """
                    _inner_class_types = {"shipping_address": ShippingAddress}

                class PriorUndisputedTransaction(StripeObject):
                    class ShippingAddress(StripeObject):
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

                    charge: str
                    """
                    Stripe charge ID for the Visa Compelling Evidence 3.0 eligible prior charge.
                    """
                    customer_account_id: Optional[str]
                    """
                    User Account ID used to log into business platform. Must be recognizable by the user.
                    """
                    customer_device_fingerprint: Optional[str]
                    """
                    Unique identifier of the cardholder's device derived from a combination of at least two hardware and software attributes. Must be at least 20 characters.
                    """
                    customer_device_id: Optional[str]
                    """
                    Unique identifier of the cardholder's device such as a device serial number (e.g., International Mobile Equipment Identity [IMEI]). Must be at least 15 characters.
                    """
                    customer_email_address: Optional[str]
                    """
                    The email address of the customer.
                    """
                    customer_purchase_ip: Optional[str]
                    """
                    The IP address that the customer used when making the purchase.
                    """
                    product_description: Optional[str]
                    """
                    A description of the product or service that was sold.
                    """
                    shipping_address: Optional[ShippingAddress]
                    """
                    The address to which a physical product was shipped. All fields are required for Visa Compelling Evidence 3.0 evidence submission.
                    """
                    _inner_class_types = {"shipping_address": ShippingAddress}

                disputed_transaction: Optional[DisputedTransaction]
                """
                Disputed transaction details for Visa Compelling Evidence 3.0 evidence submission.
                """
                prior_undisputed_transactions: List[PriorUndisputedTransaction]
                """
                List of exactly two prior undisputed transaction objects for Visa Compelling Evidence 3.0 evidence submission.
                """
                _inner_class_types = {
                    "disputed_transaction": DisputedTransaction,
                    "prior_undisputed_transactions": PriorUndisputedTransaction,
                }

            class VisaCompliance(StripeObject):
                fee_acknowledged: bool
                """
                A field acknowledging the fee incurred when countering a Visa compliance dispute. If this field is set to true, evidence can be submitted for the compliance dispute. Stripe collects a 500 USD (or local equivalent) amount to cover the network costs associated with resolving compliance disputes. Stripe refunds the 500 USD network fee if you win the dispute.
                """

            visa_compelling_evidence_3: Optional[VisaCompellingEvidence3]
            visa_compliance: Optional[VisaCompliance]
            _inner_class_types = {
                "visa_compelling_evidence_3": VisaCompellingEvidence3,
                "visa_compliance": VisaCompliance,
            }

        access_activity_log: Optional[str]
        """
        Any server or activity logs showing proof that the customer accessed or downloaded the purchased digital product. This information should include IP addresses, corresponding timestamps, and any detailed recorded activity.
        """
        billing_address: Optional[str]
        """
        The billing address provided by the customer.
        """
        cancellation_policy: Optional[ExpandableField["File"]]
        """
        (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Your subscription cancellation policy, as shown to the customer.
        """
        cancellation_policy_disclosure: Optional[str]
        """
        An explanation of how and when the customer was shown your refund policy prior to purchase.
        """
        cancellation_rebuttal: Optional[str]
        """
        A justification for why the customer's subscription was not canceled.
        """
        customer_communication: Optional[ExpandableField["File"]]
        """
        (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Any communication with the customer that you feel is relevant to your case. Examples include emails proving that the customer received the product or service, or demonstrating their use of or satisfaction with the product or service.
        """
        customer_email_address: Optional[str]
        """
        The email address of the customer.
        """
        customer_name: Optional[str]
        """
        The name of the customer.
        """
        customer_purchase_ip: Optional[str]
        """
        The IP address that the customer used when making the purchase.
        """
        customer_signature: Optional[ExpandableField["File"]]
        """
        (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) A relevant document or contract showing the customer's signature.
        """
        duplicate_charge_documentation: Optional[ExpandableField["File"]]
        """
        (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Documentation for the prior charge that can uniquely identify the charge, such as a receipt, shipping label, work order, etc. This document should be paired with a similar document from the disputed payment that proves the two payments are separate.
        """
        duplicate_charge_explanation: Optional[str]
        """
        An explanation of the difference between the disputed charge versus the prior charge that appears to be a duplicate.
        """
        duplicate_charge_id: Optional[str]
        """
        The Stripe ID for the prior charge which appears to be a duplicate of the disputed charge.
        """
        enhanced_evidence: EnhancedEvidence
        product_description: Optional[str]
        """
        A description of the product or service that was sold.
        """
        receipt: Optional[ExpandableField["File"]]
        """
        (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Any receipt or message sent to the customer notifying them of the charge.
        """
        refund_policy: Optional[ExpandableField["File"]]
        """
        (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Your refund policy, as shown to the customer.
        """
        refund_policy_disclosure: Optional[str]
        """
        Documentation demonstrating that the customer was shown your refund policy prior to purchase.
        """
        refund_refusal_explanation: Optional[str]
        """
        A justification for why the customer is not entitled to a refund.
        """
        service_date: Optional[str]
        """
        The date on which the customer received or began receiving the purchased service, in a clear human-readable format.
        """
        service_documentation: Optional[ExpandableField["File"]]
        """
        (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Documentation showing proof that a service was provided to the customer. This could include a copy of a signed contract, work order, or other form of written agreement.
        """
        shipping_address: Optional[str]
        """
        The address to which a physical product was shipped. You should try to include as complete address information as possible.
        """
        shipping_carrier: Optional[str]
        """
        The delivery service that shipped a physical product, such as Fedex, UPS, USPS, etc. If multiple carriers were used for this purchase, please separate them with commas.
        """
        shipping_date: Optional[str]
        """
        The date on which a physical product began its route to the shipping address, in a clear human-readable format.
        """
        shipping_documentation: Optional[ExpandableField["File"]]
        """
        (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Documentation showing proof that a product was shipped to the customer at the same address the customer provided to you. This could include a copy of the shipment receipt, shipping label, etc. It should show the customer's full shipping address, if possible.
        """
        shipping_tracking_number: Optional[str]
        """
        The tracking number for a physical product, obtained from the delivery service. If multiple tracking numbers were generated for this purchase, please separate them with commas.
        """
        uncategorized_file: Optional[ExpandableField["File"]]
        """
        (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Any additional evidence or statements.
        """
        uncategorized_text: Optional[str]
        """
        Any additional evidence or statements.
        """
        _inner_class_types = {"enhanced_evidence": EnhancedEvidence}

    class EvidenceDetails(StripeObject):
        class EnhancedEligibility(StripeObject):
            class VisaCompellingEvidence3(StripeObject):
                required_actions: List[
                    Literal[
                        "missing_customer_identifiers",
                        "missing_disputed_transaction_description",
                        "missing_merchandise_or_services",
                        "missing_prior_undisputed_transaction_description",
                        "missing_prior_undisputed_transactions",
                    ]
                ]
                """
                List of actions required to qualify dispute for Visa Compelling Evidence 3.0 evidence submission.
                """
                status: Literal[
                    "not_qualified", "qualified", "requires_action"
                ]
                """
                Visa Compelling Evidence 3.0 eligibility status.
                """

            class VisaCompliance(StripeObject):
                status: Literal[
                    "fee_acknowledged", "requires_fee_acknowledgement"
                ]
                """
                Visa compliance eligibility status.
                """

            visa_compelling_evidence_3: Optional[VisaCompellingEvidence3]
            visa_compliance: Optional[VisaCompliance]
            _inner_class_types = {
                "visa_compelling_evidence_3": VisaCompellingEvidence3,
                "visa_compliance": VisaCompliance,
            }

        due_by: Optional[int]
        """
        Date by which evidence must be submitted in order to successfully challenge dispute. Will be 0 if the customer's bank or credit card company doesn't allow a response for this particular dispute.
        """
        enhanced_eligibility: EnhancedEligibility
        has_evidence: bool
        """
        Whether evidence has been staged for this dispute.
        """
        past_due: bool
        """
        Whether the last evidence submission was submitted past the due date. Defaults to `false` if no evidence submissions have occurred. If `true`, then delivery of the latest evidence is *not* guaranteed.
        """
        submission_count: int
        """
        The number of times evidence has been submitted. Typically, you may only submit evidence once.
        """
        _inner_class_types = {"enhanced_eligibility": EnhancedEligibility}

    class PaymentMethodDetails(StripeObject):
        class AmazonPay(StripeObject):
            dispute_type: Optional[Literal["chargeback", "claim"]]
            """
            The AmazonPay dispute type, chargeback or claim
            """

        class Card(StripeObject):
            brand: str
            """
            Card brand. Can be `amex`, `cartes_bancaires`, `diners`, `discover`, `eftpos_au`, `jcb`, `link`, `mastercard`, `unionpay`, `visa` or `unknown`.
            """
            case_type: Literal[
                "block", "chargeback", "compliance", "inquiry", "resolution"
            ]
            """
            The type of dispute opened. Different case types may have varying fees and financial impact.
            """
            network_reason_code: Optional[str]
            """
            The card network's specific dispute reason code, which maps to one of Stripe's primary dispute categories to simplify response guidance. The [Network code map](https://stripe.com/docs/disputes/categories#network-code-map) lists all available dispute reason codes by network.
            """

        class Klarna(StripeObject):
            chargeback_loss_reason_code: Optional[str]
            """
            Chargeback loss reason mapped by Stripe from Klarna's chargeback loss reason
            """
            reason_code: Optional[str]
            """
            The reason for the dispute as defined by Klarna
            """

        class Paypal(StripeObject):
            case_id: Optional[str]
            """
            The ID of the dispute in PayPal.
            """
            reason_code: Optional[str]
            """
            The reason for the dispute as defined by PayPal
            """

        amazon_pay: Optional[AmazonPay]
        card: Optional[Card]
        klarna: Optional[Klarna]
        paypal: Optional[Paypal]
        type: Literal["amazon_pay", "card", "klarna", "paypal"]
        """
        Payment method type.
        """
        _inner_class_types = {
            "amazon_pay": AmazonPay,
            "card": Card,
            "klarna": Klarna,
            "paypal": Paypal,
        }

    amount: int
    """
    Disputed amount. Usually the amount of the charge, but it can differ (usually because of currency fluctuation or because only part of the order is disputed).
    """
    balance_transactions: List["BalanceTransaction"]
    """
    List of zero, one, or two balance transactions that show funds withdrawn and reinstated to your Stripe account as a result of this dispute.
    """
    charge: ExpandableField["Charge"]
    """
    ID of the charge that's disputed.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    enhanced_eligibility_types: List[
        Literal["visa_compelling_evidence_3", "visa_compliance"]
    ]
    """
    List of eligibility types that are included in `enhanced_evidence`.
    """
    evidence: Evidence
    evidence_details: EvidenceDetails
    id: str
    """
    Unique identifier for the object.
    """
    is_charge_refundable: bool
    """
    If true, it's still possible to refund the disputed payment. After the payment has been fully refunded, no further funds are withdrawn from your Stripe account as a result of this dispute.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    network_reason_code: Optional[str]
    """
    Network-dependent reason code for the dispute.
    """
    object: Literal["dispute"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    payment_intent: Optional[ExpandableField["PaymentIntent"]]
    """
    ID of the PaymentIntent that's disputed.
    """
    payment_method_details: Optional[PaymentMethodDetails]
    reason: str
    """
    Reason given by cardholder for dispute. Possible values are `bank_cannot_process`, `check_returned`, `credit_not_processed`, `customer_initiated`, `debit_not_authorized`, `duplicate`, `fraudulent`, `general`, `incorrect_account_details`, `insufficient_funds`, `noncompliant`, `product_not_received`, `product_unacceptable`, `subscription_canceled`, or `unrecognized`. Learn more about [dispute reasons](https://docs.stripe.com/disputes/categories).
    """
    status: Literal[
        "lost",
        "needs_response",
        "prevented",
        "under_review",
        "warning_closed",
        "warning_needs_response",
        "warning_under_review",
        "won",
    ]
    """
    The current status of a dispute. Possible values include:`warning_needs_response`, `warning_under_review`, `warning_closed`, `needs_response`, `under_review`, `won`, `lost`, or `prevented`.
    """

    @classmethod
    def _cls_close(
        cls, dispute: str, **params: Unpack["DisputeCloseParams"]
    ) -> "Dispute":
        """
        Closing the dispute for a charge indicates that you do not have any evidence to submit and are essentially dismissing the dispute, acknowledging it as lost.

        The status of the dispute will change from needs_response to lost. Closing a dispute is irreversible.
        """
        return cast(
            "Dispute",
            cls._static_request(
                "post",
                "/v1/disputes/{dispute}/close".format(
                    dispute=sanitize_id(dispute)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def close(
        dispute: str, **params: Unpack["DisputeCloseParams"]
    ) -> "Dispute":
        """
        Closing the dispute for a charge indicates that you do not have any evidence to submit and are essentially dismissing the dispute, acknowledging it as lost.

        The status of the dispute will change from needs_response to lost. Closing a dispute is irreversible.
        """
        ...

    @overload
    def close(self, **params: Unpack["DisputeCloseParams"]) -> "Dispute":
        """
        Closing the dispute for a charge indicates that you do not have any evidence to submit and are essentially dismissing the dispute, acknowledging it as lost.

        The status of the dispute will change from needs_response to lost. Closing a dispute is irreversible.
        """
        ...

    @class_method_variant("_cls_close")
    def close(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["DisputeCloseParams"]
    ) -> "Dispute":
        """
        Closing the dispute for a charge indicates that you do not have any evidence to submit and are essentially dismissing the dispute, acknowledging it as lost.

        The status of the dispute will change from needs_response to lost. Closing a dispute is irreversible.
        """
        return cast(
            "Dispute",
            self._request(
                "post",
                "/v1/disputes/{dispute}/close".format(
                    dispute=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_close_async(
        cls, dispute: str, **params: Unpack["DisputeCloseParams"]
    ) -> "Dispute":
        """
        Closing the dispute for a charge indicates that you do not have any evidence to submit and are essentially dismissing the dispute, acknowledging it as lost.

        The status of the dispute will change from needs_response to lost. Closing a dispute is irreversible.
        """
        return cast(
            "Dispute",
            await cls._static_request_async(
                "post",
                "/v1/disputes/{dispute}/close".format(
                    dispute=sanitize_id(dispute)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def close_async(
        dispute: str, **params: Unpack["DisputeCloseParams"]
    ) -> "Dispute":
        """
        Closing the dispute for a charge indicates that you do not have any evidence to submit and are essentially dismissing the dispute, acknowledging it as lost.

        The status of the dispute will change from needs_response to lost. Closing a dispute is irreversible.
        """
        ...

    @overload
    async def close_async(
        self, **params: Unpack["DisputeCloseParams"]
    ) -> "Dispute":
        """
        Closing the dispute for a charge indicates that you do not have any evidence to submit and are essentially dismissing the dispute, acknowledging it as lost.

        The status of the dispute will change from needs_response to lost. Closing a dispute is irreversible.
        """
        ...

    @class_method_variant("_cls_close_async")
    async def close_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["DisputeCloseParams"]
    ) -> "Dispute":
        """
        Closing the dispute for a charge indicates that you do not have any evidence to submit and are essentially dismissing the dispute, acknowledging it as lost.

        The status of the dispute will change from needs_response to lost. Closing a dispute is irreversible.
        """
        return cast(
            "Dispute",
            await self._request_async(
                "post",
                "/v1/disputes/{dispute}/close".format(
                    dispute=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["DisputeListParams"]
    ) -> ListObject["Dispute"]:
        """
        Returns a list of your disputes.
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
        cls, **params: Unpack["DisputeListParams"]
    ) -> ListObject["Dispute"]:
        """
        Returns a list of your disputes.
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
        cls, id: str, **params: Unpack["DisputeModifyParams"]
    ) -> "Dispute":
        """
        When you get a dispute, contacting your customer is always the best first step. If that doesn't work, you can submit evidence to help us resolve the dispute in your favor. You can do this in your [dashboard](https://dashboard.stripe.com/disputes), but if you prefer, you can use the API to submit evidence programmatically.

        Depending on your dispute type, different evidence fields will give you a better chance of winning your dispute. To figure out which evidence fields to provide, see our [guide to dispute types](https://docs.stripe.com/docs/disputes/categories).
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Dispute",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["DisputeModifyParams"]
    ) -> "Dispute":
        """
        When you get a dispute, contacting your customer is always the best first step. If that doesn't work, you can submit evidence to help us resolve the dispute in your favor. You can do this in your [dashboard](https://dashboard.stripe.com/disputes), but if you prefer, you can use the API to submit evidence programmatically.

        Depending on your dispute type, different evidence fields will give you a better chance of winning your dispute. To figure out which evidence fields to provide, see our [guide to dispute types](https://docs.stripe.com/docs/disputes/categories).
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Dispute",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["DisputeRetrieveParams"]
    ) -> "Dispute":
        """
        Retrieves the dispute with the given ID.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["DisputeRetrieveParams"]
    ) -> "Dispute":
        """
        Retrieves the dispute with the given ID.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "evidence": Evidence,
        "evidence_details": EvidenceDetails,
        "payment_method_details": PaymentMethodDetails,
    }
