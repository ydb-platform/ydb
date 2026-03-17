# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class DisputeModifyParams(RequestOptions):
    amount: NotRequired[int]
    """
    The dispute amount in the card's currency and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    evidence: NotRequired["DisputeModifyParamsEvidence"]
    """
    Evidence provided for the dispute.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """


class DisputeModifyParamsEvidence(TypedDict):
    canceled: NotRequired["Literal['']|DisputeModifyParamsEvidenceCanceled"]
    """
    Evidence provided when `reason` is 'canceled'.
    """
    duplicate: NotRequired["Literal['']|DisputeModifyParamsEvidenceDuplicate"]
    """
    Evidence provided when `reason` is 'duplicate'.
    """
    fraudulent: NotRequired[
        "Literal['']|DisputeModifyParamsEvidenceFraudulent"
    ]
    """
    Evidence provided when `reason` is 'fraudulent'.
    """
    merchandise_not_as_described: NotRequired[
        "Literal['']|DisputeModifyParamsEvidenceMerchandiseNotAsDescribed"
    ]
    """
    Evidence provided when `reason` is 'merchandise_not_as_described'.
    """
    no_valid_authorization: NotRequired[
        "Literal['']|DisputeModifyParamsEvidenceNoValidAuthorization"
    ]
    """
    Evidence provided when `reason` is 'no_valid_authorization'.
    """
    not_received: NotRequired[
        "Literal['']|DisputeModifyParamsEvidenceNotReceived"
    ]
    """
    Evidence provided when `reason` is 'not_received'.
    """
    other: NotRequired["Literal['']|DisputeModifyParamsEvidenceOther"]
    """
    Evidence provided when `reason` is 'other'.
    """
    reason: NotRequired[
        Literal[
            "canceled",
            "duplicate",
            "fraudulent",
            "merchandise_not_as_described",
            "no_valid_authorization",
            "not_received",
            "other",
            "service_not_as_described",
        ]
    ]
    """
    The reason for filing the dispute. The evidence should be submitted in the field of the same name.
    """
    service_not_as_described: NotRequired[
        "Literal['']|DisputeModifyParamsEvidenceServiceNotAsDescribed"
    ]
    """
    Evidence provided when `reason` is 'service_not_as_described'.
    """


class DisputeModifyParamsEvidenceCanceled(TypedDict):
    additional_documentation: NotRequired["Literal['']|str"]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
    """
    canceled_at: NotRequired["Literal['']|int"]
    """
    Date when order was canceled.
    """
    cancellation_policy_provided: NotRequired["Literal['']|bool"]
    """
    Whether the cardholder was provided with a cancellation policy.
    """
    cancellation_reason: NotRequired["Literal['']|str"]
    """
    Reason for canceling the order.
    """
    expected_at: NotRequired["Literal['']|int"]
    """
    Date when the cardholder expected to receive the product.
    """
    explanation: NotRequired["Literal['']|str"]
    """
    Explanation of why the cardholder is disputing this transaction.
    """
    product_description: NotRequired["Literal['']|str"]
    """
    Description of the merchandise or service that was purchased.
    """
    product_type: NotRequired["Literal['']|Literal['merchandise', 'service']"]
    """
    Whether the product was a merchandise or service.
    """
    return_status: NotRequired[
        "Literal['']|Literal['merchant_rejected', 'successful']"
    ]
    """
    Result of cardholder's attempt to return the product.
    """
    returned_at: NotRequired["Literal['']|int"]
    """
    Date when the product was returned or attempted to be returned.
    """


class DisputeModifyParamsEvidenceDuplicate(TypedDict):
    additional_documentation: NotRequired["Literal['']|str"]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
    """
    card_statement: NotRequired["Literal['']|str"]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Copy of the card statement showing that the product had already been paid for.
    """
    cash_receipt: NotRequired["Literal['']|str"]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Copy of the receipt showing that the product had been paid for in cash.
    """
    check_image: NotRequired["Literal['']|str"]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Image of the front and back of the check that was used to pay for the product.
    """
    explanation: NotRequired["Literal['']|str"]
    """
    Explanation of why the cardholder is disputing this transaction.
    """
    original_transaction: NotRequired[str]
    """
    Transaction (e.g., ipi_...) that the disputed transaction is a duplicate of. Of the two or more transactions that are copies of each other, this is original undisputed one.
    """


class DisputeModifyParamsEvidenceFraudulent(TypedDict):
    additional_documentation: NotRequired["Literal['']|str"]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
    """
    explanation: NotRequired["Literal['']|str"]
    """
    Explanation of why the cardholder is disputing this transaction.
    """


class DisputeModifyParamsEvidenceMerchandiseNotAsDescribed(TypedDict):
    additional_documentation: NotRequired["Literal['']|str"]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
    """
    explanation: NotRequired["Literal['']|str"]
    """
    Explanation of why the cardholder is disputing this transaction.
    """
    received_at: NotRequired["Literal['']|int"]
    """
    Date when the product was received.
    """
    return_description: NotRequired["Literal['']|str"]
    """
    Description of the cardholder's attempt to return the product.
    """
    return_status: NotRequired[
        "Literal['']|Literal['merchant_rejected', 'successful']"
    ]
    """
    Result of cardholder's attempt to return the product.
    """
    returned_at: NotRequired["Literal['']|int"]
    """
    Date when the product was returned or attempted to be returned.
    """


class DisputeModifyParamsEvidenceNoValidAuthorization(TypedDict):
    additional_documentation: NotRequired["Literal['']|str"]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
    """
    explanation: NotRequired["Literal['']|str"]
    """
    Explanation of why the cardholder is disputing this transaction.
    """


class DisputeModifyParamsEvidenceNotReceived(TypedDict):
    additional_documentation: NotRequired["Literal['']|str"]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
    """
    expected_at: NotRequired["Literal['']|int"]
    """
    Date when the cardholder expected to receive the product.
    """
    explanation: NotRequired["Literal['']|str"]
    """
    Explanation of why the cardholder is disputing this transaction.
    """
    product_description: NotRequired["Literal['']|str"]
    """
    Description of the merchandise or service that was purchased.
    """
    product_type: NotRequired["Literal['']|Literal['merchandise', 'service']"]
    """
    Whether the product was a merchandise or service.
    """


class DisputeModifyParamsEvidenceOther(TypedDict):
    additional_documentation: NotRequired["Literal['']|str"]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
    """
    explanation: NotRequired["Literal['']|str"]
    """
    Explanation of why the cardholder is disputing this transaction.
    """
    product_description: NotRequired["Literal['']|str"]
    """
    Description of the merchandise or service that was purchased.
    """
    product_type: NotRequired["Literal['']|Literal['merchandise', 'service']"]
    """
    Whether the product was a merchandise or service.
    """


class DisputeModifyParamsEvidenceServiceNotAsDescribed(TypedDict):
    additional_documentation: NotRequired["Literal['']|str"]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
    """
    canceled_at: NotRequired["Literal['']|int"]
    """
    Date when order was canceled.
    """
    cancellation_reason: NotRequired["Literal['']|str"]
    """
    Reason for canceling the order.
    """
    explanation: NotRequired["Literal['']|str"]
    """
    Explanation of why the cardholder is disputing this transaction.
    """
    received_at: NotRequired["Literal['']|int"]
    """
    Date when the product was received.
    """
