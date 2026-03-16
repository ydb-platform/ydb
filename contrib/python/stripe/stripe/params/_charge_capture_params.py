# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired, TypedDict


class ChargeCaptureParams(RequestOptions):
    amount: NotRequired[int]
    """
    The amount to capture, which must be less than or equal to the original amount.
    """
    application_fee: NotRequired[int]
    """
    An application fee to add on to this charge.
    """
    application_fee_amount: NotRequired[int]
    """
    An application fee amount to add on to this charge, which must be less than or equal to the original amount.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    receipt_email: NotRequired[str]
    """
    The email address to send this charge's receipt to. This will override the previously-specified email address for this charge, if one was set. Receipts will not be sent in test mode.
    """
    statement_descriptor: NotRequired[str]
    """
    For a non-card charge, text that appears on the customer's statement as the statement descriptor. This value overrides the account's default statement descriptor. For information about requirements, including the 22-character limit, see [the Statement Descriptor docs](https://docs.stripe.com/get-started/account/statement-descriptors).

    For a card charge, this value is ignored unless you don't specify a `statement_descriptor_suffix`, in which case this value is used as the suffix.
    """
    statement_descriptor_suffix: NotRequired[str]
    """
    Provides information about a card charge. Concatenated to the account's [statement descriptor prefix](https://docs.stripe.com/get-started/account/statement-descriptors#static) to form the complete statement descriptor that appears on the customer's statement. If the account has no prefix value, the suffix is concatenated to the account's statement descriptor.
    """
    transfer_data: NotRequired["ChargeCaptureParamsTransferData"]
    """
    An optional dictionary including the account to automatically transfer to as part of a destination charge. [See the Connect documentation](https://docs.stripe.com/connect/destination-charges) for details.
    """
    transfer_group: NotRequired[str]
    """
    A string that identifies this transaction as part of a group. `transfer_group` may only be provided if it has not been set. See the [Connect documentation](https://docs.stripe.com/connect/separate-charges-and-transfers#transfer-options) for details.
    """


class ChargeCaptureParamsTransferData(TypedDict):
    amount: NotRequired[int]
    """
    The amount transferred to the destination account, if specified. By default, the entire charge amount is transferred to the destination account.
    """
