# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class DisputeUpdateParams(TypedDict):
    evidence: NotRequired["DisputeUpdateParamsEvidence"]
    """
    Evidence to upload, to respond to a dispute. Updating any field in the hash will submit all fields in the hash for review. The combined character count of all fields is limited to 150,000.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    submit: NotRequired[bool]
    """
    Whether to immediately submit evidence to the bank. If `false`, evidence is staged on the dispute. Staged evidence is visible in the API and Dashboard, and can be submitted to the bank by making another request with this attribute set to `true` (the default).
    """


class DisputeUpdateParamsEvidence(TypedDict):
    access_activity_log: NotRequired[str]
    """
    Any server or activity logs showing proof that the customer accessed or downloaded the purchased digital product. This information should include IP addresses, corresponding timestamps, and any detailed recorded activity. Has a maximum character count of 20,000.
    """
    billing_address: NotRequired[str]
    """
    The billing address provided by the customer.
    """
    cancellation_policy: NotRequired[str]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Your subscription cancellation policy, as shown to the customer.
    """
    cancellation_policy_disclosure: NotRequired[str]
    """
    An explanation of how and when the customer was shown your refund policy prior to purchase. Has a maximum character count of 20,000.
    """
    cancellation_rebuttal: NotRequired[str]
    """
    A justification for why the customer's subscription was not canceled. Has a maximum character count of 20,000.
    """
    customer_communication: NotRequired[str]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Any communication with the customer that you feel is relevant to your case. Examples include emails proving that the customer received the product or service, or demonstrating their use of or satisfaction with the product or service.
    """
    customer_email_address: NotRequired[str]
    """
    The email address of the customer.
    """
    customer_name: NotRequired[str]
    """
    The name of the customer.
    """
    customer_purchase_ip: NotRequired[str]
    """
    The IP address that the customer used when making the purchase.
    """
    customer_signature: NotRequired[str]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) A relevant document or contract showing the customer's signature.
    """
    duplicate_charge_documentation: NotRequired[str]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Documentation for the prior charge that can uniquely identify the charge, such as a receipt, shipping label, work order, etc. This document should be paired with a similar document from the disputed payment that proves the two payments are separate.
    """
    duplicate_charge_explanation: NotRequired[str]
    """
    An explanation of the difference between the disputed charge versus the prior charge that appears to be a duplicate. Has a maximum character count of 20,000.
    """
    duplicate_charge_id: NotRequired[str]
    """
    The Stripe ID for the prior charge which appears to be a duplicate of the disputed charge.
    """
    enhanced_evidence: NotRequired[
        "Literal['']|DisputeUpdateParamsEvidenceEnhancedEvidence"
    ]
    """
    Additional evidence for qualifying evidence programs.
    """
    product_description: NotRequired[str]
    """
    A description of the product or service that was sold. Has a maximum character count of 20,000.
    """
    receipt: NotRequired[str]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Any receipt or message sent to the customer notifying them of the charge.
    """
    refund_policy: NotRequired[str]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Your refund policy, as shown to the customer.
    """
    refund_policy_disclosure: NotRequired[str]
    """
    Documentation demonstrating that the customer was shown your refund policy prior to purchase. Has a maximum character count of 20,000.
    """
    refund_refusal_explanation: NotRequired[str]
    """
    A justification for why the customer is not entitled to a refund. Has a maximum character count of 20,000.
    """
    service_date: NotRequired[str]
    """
    The date on which the customer received or began receiving the purchased service, in a clear human-readable format.
    """
    service_documentation: NotRequired[str]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Documentation showing proof that a service was provided to the customer. This could include a copy of a signed contract, work order, or other form of written agreement.
    """
    shipping_address: NotRequired[str]
    """
    The address to which a physical product was shipped. You should try to include as complete address information as possible.
    """
    shipping_carrier: NotRequired[str]
    """
    The delivery service that shipped a physical product, such as Fedex, UPS, USPS, etc. If multiple carriers were used for this purchase, please separate them with commas.
    """
    shipping_date: NotRequired[str]
    """
    The date on which a physical product began its route to the shipping address, in a clear human-readable format.
    """
    shipping_documentation: NotRequired[str]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Documentation showing proof that a product was shipped to the customer at the same address the customer provided to you. This could include a copy of the shipment receipt, shipping label, etc. It should show the customer's full shipping address, if possible.
    """
    shipping_tracking_number: NotRequired[str]
    """
    The tracking number for a physical product, obtained from the delivery service. If multiple tracking numbers were generated for this purchase, please separate them with commas.
    """
    uncategorized_file: NotRequired[str]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Any additional evidence or statements.
    """
    uncategorized_text: NotRequired[str]
    """
    Any additional evidence or statements. Has a maximum character count of 20,000.
    """


class DisputeUpdateParamsEvidenceEnhancedEvidence(TypedDict):
    visa_compelling_evidence_3: NotRequired[
        "DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3"
    ]
    """
    Evidence provided for Visa Compelling Evidence 3.0 evidence submission.
    """
    visa_compliance: NotRequired[
        "DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompliance"
    ]
    """
    Evidence provided for Visa compliance evidence submission.
    """


class DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3(
    TypedDict,
):
    disputed_transaction: NotRequired[
        "DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransaction"
    ]
    """
    Disputed transaction details for Visa Compelling Evidence 3.0 evidence submission.
    """
    prior_undisputed_transactions: NotRequired[
        List[
            "DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransaction"
        ]
    ]
    """
    List of exactly two prior undisputed transaction objects for Visa Compelling Evidence 3.0 evidence submission.
    """


class DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransaction(
    TypedDict,
):
    customer_account_id: NotRequired["Literal['']|str"]
    """
    User Account ID used to log into business platform. Must be recognizable by the user.
    """
    customer_device_fingerprint: NotRequired["Literal['']|str"]
    """
    Unique identifier of the cardholder's device derived from a combination of at least two hardware and software attributes. Must be at least 20 characters.
    """
    customer_device_id: NotRequired["Literal['']|str"]
    """
    Unique identifier of the cardholder's device such as a device serial number (e.g., International Mobile Equipment Identity [IMEI]). Must be at least 15 characters.
    """
    customer_email_address: NotRequired["Literal['']|str"]
    """
    The email address of the customer.
    """
    customer_purchase_ip: NotRequired["Literal['']|str"]
    """
    The IP address that the customer used when making the purchase.
    """
    merchandise_or_services: NotRequired[Literal["merchandise", "services"]]
    """
    Categorization of disputed payment.
    """
    product_description: NotRequired["Literal['']|str"]
    """
    A description of the product or service that was sold.
    """
    shipping_address: NotRequired[
        "DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransactionShippingAddress"
    ]
    """
    The address to which a physical product was shipped. All fields are required for Visa Compelling Evidence 3.0 evidence submission.
    """


class DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransactionShippingAddress(
    TypedDict,
):
    city: NotRequired["Literal['']|str"]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired["Literal['']|str"]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired["Literal['']|str"]
    """
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired["Literal['']|str"]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired["Literal['']|str"]
    """
    ZIP or postal code.
    """
    state: NotRequired["Literal['']|str"]
    """
    State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """


class DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransaction(
    TypedDict,
):
    charge: str
    """
    Stripe charge ID for the Visa Compelling Evidence 3.0 eligible prior charge.
    """
    customer_account_id: NotRequired["Literal['']|str"]
    """
    User Account ID used to log into business platform. Must be recognizable by the user.
    """
    customer_device_fingerprint: NotRequired["Literal['']|str"]
    """
    Unique identifier of the cardholder's device derived from a combination of at least two hardware and software attributes. Must be at least 20 characters.
    """
    customer_device_id: NotRequired["Literal['']|str"]
    """
    Unique identifier of the cardholder's device such as a device serial number (e.g., International Mobile Equipment Identity [IMEI]). Must be at least 15 characters.
    """
    customer_email_address: NotRequired["Literal['']|str"]
    """
    The email address of the customer.
    """
    customer_purchase_ip: NotRequired["Literal['']|str"]
    """
    The IP address that the customer used when making the purchase.
    """
    product_description: NotRequired["Literal['']|str"]
    """
    A description of the product or service that was sold.
    """
    shipping_address: NotRequired[
        "DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransactionShippingAddress"
    ]
    """
    The address to which a physical product was shipped. All fields are required for Visa Compelling Evidence 3.0 evidence submission.
    """


class DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransactionShippingAddress(
    TypedDict,
):
    city: NotRequired["Literal['']|str"]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired["Literal['']|str"]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired["Literal['']|str"]
    """
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired["Literal['']|str"]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired["Literal['']|str"]
    """
    ZIP or postal code.
    """
    state: NotRequired["Literal['']|str"]
    """
    State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """


class DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompliance(TypedDict):
    fee_acknowledged: NotRequired[bool]
    """
    A field acknowledging the fee incurred when countering a Visa compliance dispute. If this field is set to true, evidence can be submitted for the compliance dispute. Stripe collects a 500 USD (or local equivalent) amount to cover the network costs associated with resolving compliance disputes. Stripe refunds the 500 USD network fee if you win the dispute.
    """
