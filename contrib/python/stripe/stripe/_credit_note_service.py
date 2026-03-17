# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._credit_note import CreditNote
    from stripe._credit_note_line_item_service import CreditNoteLineItemService
    from stripe._credit_note_preview_lines_service import (
        CreditNotePreviewLinesService,
    )
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._credit_note_create_params import CreditNoteCreateParams
    from stripe.params._credit_note_list_params import CreditNoteListParams
    from stripe.params._credit_note_preview_params import (
        CreditNotePreviewParams,
    )
    from stripe.params._credit_note_retrieve_params import (
        CreditNoteRetrieveParams,
    )
    from stripe.params._credit_note_update_params import CreditNoteUpdateParams
    from stripe.params._credit_note_void_credit_note_params import (
        CreditNoteVoidCreditNoteParams,
    )

_subservices = {
    "line_items": [
        "stripe._credit_note_line_item_service",
        "CreditNoteLineItemService",
    ],
    "preview_lines": [
        "stripe._credit_note_preview_lines_service",
        "CreditNotePreviewLinesService",
    ],
}


class CreditNoteService(StripeService):
    line_items: "CreditNoteLineItemService"
    preview_lines: "CreditNotePreviewLinesService"

    def __init__(self, requestor):
        super().__init__(requestor)

    def __getattr__(self, name):
        try:
            import_from, service = _subservices[name]
            service_class = getattr(
                import_module(import_from),
                service,
            )
            setattr(
                self,
                name,
                service_class(self._requestor),
            )
            return getattr(self, name)
        except KeyError:
            raise AttributeError()

    def list(
        self,
        params: Optional["CreditNoteListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CreditNote]":
        """
        Returns a list of credit notes.
        """
        return cast(
            "ListObject[CreditNote]",
            self._request(
                "get",
                "/v1/credit_notes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["CreditNoteListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CreditNote]":
        """
        Returns a list of credit notes.
        """
        return cast(
            "ListObject[CreditNote]",
            await self._request_async(
                "get",
                "/v1/credit_notes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "CreditNoteCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CreditNote":
        """
        Issue a credit note to adjust the amount of a finalized invoice. A credit note will first reduce the invoice's amount_remaining (and amount_due), but not below zero.
        This amount is indicated by the credit note's pre_payment_amount. The excess amount is indicated by post_payment_amount, and it can result in any combination of the following:


        Refunds: create a new refund (using refund_amount) or link existing refunds (using refunds).
        Customer balance credit: credit the customer's balance (using credit_amount) which will be automatically applied to their next invoice when it's finalized.
        Outside of Stripe credit: record the amount that is or will be credited outside of Stripe (using out_of_band_amount).


        The sum of refunds, customer balance credits, and outside of Stripe credits must equal the post_payment_amount.

        You may issue multiple credit notes for an invoice. Each credit note may increment the invoice's pre_payment_credit_notes_amount,
        post_payment_credit_notes_amount, or both, depending on the invoice's amount_remaining at the time of credit note creation.
        """
        return cast(
            "CreditNote",
            self._request(
                "post",
                "/v1/credit_notes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "CreditNoteCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CreditNote":
        """
        Issue a credit note to adjust the amount of a finalized invoice. A credit note will first reduce the invoice's amount_remaining (and amount_due), but not below zero.
        This amount is indicated by the credit note's pre_payment_amount. The excess amount is indicated by post_payment_amount, and it can result in any combination of the following:


        Refunds: create a new refund (using refund_amount) or link existing refunds (using refunds).
        Customer balance credit: credit the customer's balance (using credit_amount) which will be automatically applied to their next invoice when it's finalized.
        Outside of Stripe credit: record the amount that is or will be credited outside of Stripe (using out_of_band_amount).


        The sum of refunds, customer balance credits, and outside of Stripe credits must equal the post_payment_amount.

        You may issue multiple credit notes for an invoice. Each credit note may increment the invoice's pre_payment_credit_notes_amount,
        post_payment_credit_notes_amount, or both, depending on the invoice's amount_remaining at the time of credit note creation.
        """
        return cast(
            "CreditNote",
            await self._request_async(
                "post",
                "/v1/credit_notes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["CreditNoteRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditNote":
        """
        Retrieves the credit note object with the given identifier.
        """
        return cast(
            "CreditNote",
            self._request(
                "get",
                "/v1/credit_notes/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["CreditNoteRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditNote":
        """
        Retrieves the credit note object with the given identifier.
        """
        return cast(
            "CreditNote",
            await self._request_async(
                "get",
                "/v1/credit_notes/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        id: str,
        params: Optional["CreditNoteUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditNote":
        """
        Updates an existing credit note.
        """
        return cast(
            "CreditNote",
            self._request(
                "post",
                "/v1/credit_notes/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        id: str,
        params: Optional["CreditNoteUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditNote":
        """
        Updates an existing credit note.
        """
        return cast(
            "CreditNote",
            await self._request_async(
                "post",
                "/v1/credit_notes/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def preview(
        self,
        params: "CreditNotePreviewParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CreditNote":
        """
        Get a preview of a credit note without creating it.
        """
        return cast(
            "CreditNote",
            self._request(
                "get",
                "/v1/credit_notes/preview",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def preview_async(
        self,
        params: "CreditNotePreviewParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CreditNote":
        """
        Get a preview of a credit note without creating it.
        """
        return cast(
            "CreditNote",
            await self._request_async(
                "get",
                "/v1/credit_notes/preview",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def void_credit_note(
        self,
        id: str,
        params: Optional["CreditNoteVoidCreditNoteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditNote":
        """
        Marks a credit note as void. Learn more about [voiding credit notes](https://docs.stripe.com/docs/billing/invoices/credit-notes#voiding).
        """
        return cast(
            "CreditNote",
            self._request(
                "post",
                "/v1/credit_notes/{id}/void".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def void_credit_note_async(
        self,
        id: str,
        params: Optional["CreditNoteVoidCreditNoteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditNote":
        """
        Marks a credit note as void. Learn more about [voiding credit notes](https://docs.stripe.com/docs/billing/invoices/credit-notes#voiding).
        """
        return cast(
            "CreditNote",
            await self._request_async(
                "post",
                "/v1/credit_notes/{id}/void".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
