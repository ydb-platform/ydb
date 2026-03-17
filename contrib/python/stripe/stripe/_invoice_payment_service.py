# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._invoice_payment import InvoicePayment
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._invoice_payment_list_params import (
        InvoicePaymentListParams,
    )
    from stripe.params._invoice_payment_retrieve_params import (
        InvoicePaymentRetrieveParams,
    )


class InvoicePaymentService(StripeService):
    def list(
        self,
        params: Optional["InvoicePaymentListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[InvoicePayment]":
        """
        When retrieving an invoice, there is an includable payments property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of payments.
        """
        return cast(
            "ListObject[InvoicePayment]",
            self._request(
                "get",
                "/v1/invoice_payments",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["InvoicePaymentListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[InvoicePayment]":
        """
        When retrieving an invoice, there is an includable payments property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of payments.
        """
        return cast(
            "ListObject[InvoicePayment]",
            await self._request_async(
                "get",
                "/v1/invoice_payments",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        invoice_payment: str,
        params: Optional["InvoicePaymentRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InvoicePayment":
        """
        Retrieves the invoice payment with the given ID.
        """
        return cast(
            "InvoicePayment",
            self._request(
                "get",
                "/v1/invoice_payments/{invoice_payment}".format(
                    invoice_payment=sanitize_id(invoice_payment),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        invoice_payment: str,
        params: Optional["InvoicePaymentRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InvoicePayment":
        """
        Retrieves the invoice payment with the given ID.
        """
        return cast(
            "InvoicePayment",
            await self._request_async(
                "get",
                "/v1/invoice_payments/{invoice_payment}".format(
                    invoice_payment=sanitize_id(invoice_payment),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
