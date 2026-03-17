# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired


class InvoicePayParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    forgive: NotRequired[bool]
    """
    In cases where the source used to pay the invoice has insufficient funds, passing `forgive=true` controls whether a charge should be attempted for the full amount available on the source, up to the amount to fully pay the invoice. This effectively forgives the difference between the amount available on the source and the amount due.

    Passing `forgive=false` will fail the charge if the source hasn't been pre-funded with the right amount. An example for this case is with ACH Credit Transfers and wires: if the amount wired is less than the amount due by a small amount, you might want to forgive the difference. Defaults to `false`.
    """
    mandate: NotRequired["Literal['']|str"]
    """
    ID of the mandate to be used for this invoice. It must correspond to the payment method used to pay the invoice, including the payment_method param or the invoice's default_payment_method or default_source, if set.
    """
    off_session: NotRequired[bool]
    """
    Indicates if a customer is on or off-session while an invoice payment is attempted. Defaults to `true` (off-session).
    """
    paid_out_of_band: NotRequired[bool]
    """
    Boolean representing whether an invoice is paid outside of Stripe. This will result in no charge being made. Defaults to `false`.
    """
    payment_method: NotRequired[str]
    """
    A PaymentMethod to be charged. The PaymentMethod must be the ID of a PaymentMethod belonging to the customer associated with the invoice being paid.
    """
    source: NotRequired[str]
    """
    A payment source to be charged. The source must be the ID of a source belonging to the customer associated with the invoice being paid.
    """
