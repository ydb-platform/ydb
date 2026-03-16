# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._invoice import Invoice
    from stripe._invoice_line_item_service import InvoiceLineItemService
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._search_result_object import SearchResultObject
    from stripe.params._invoice_add_lines_params import InvoiceAddLinesParams
    from stripe.params._invoice_attach_payment_params import (
        InvoiceAttachPaymentParams,
    )
    from stripe.params._invoice_create_params import InvoiceCreateParams
    from stripe.params._invoice_create_preview_params import (
        InvoiceCreatePreviewParams,
    )
    from stripe.params._invoice_delete_params import InvoiceDeleteParams
    from stripe.params._invoice_finalize_invoice_params import (
        InvoiceFinalizeInvoiceParams,
    )
    from stripe.params._invoice_list_params import InvoiceListParams
    from stripe.params._invoice_mark_uncollectible_params import (
        InvoiceMarkUncollectibleParams,
    )
    from stripe.params._invoice_pay_params import InvoicePayParams
    from stripe.params._invoice_remove_lines_params import (
        InvoiceRemoveLinesParams,
    )
    from stripe.params._invoice_retrieve_params import InvoiceRetrieveParams
    from stripe.params._invoice_search_params import InvoiceSearchParams
    from stripe.params._invoice_send_invoice_params import (
        InvoiceSendInvoiceParams,
    )
    from stripe.params._invoice_update_lines_params import (
        InvoiceUpdateLinesParams,
    )
    from stripe.params._invoice_update_params import InvoiceUpdateParams
    from stripe.params._invoice_void_invoice_params import (
        InvoiceVoidInvoiceParams,
    )

_subservices = {
    "line_items": [
        "stripe._invoice_line_item_service",
        "InvoiceLineItemService",
    ],
}


class InvoiceService(StripeService):
    line_items: "InvoiceLineItemService"

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

    def delete(
        self,
        invoice: str,
        params: Optional["InvoiceDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Permanently deletes a one-off invoice draft. This cannot be undone. Attempts to delete invoices that are no longer in a draft state will fail; once an invoice has been finalized or if an invoice is for a subscription, it must be [voided](https://docs.stripe.com/api#void_invoice).
        """
        return cast(
            "Invoice",
            self._request(
                "delete",
                "/v1/invoices/{invoice}".format(invoice=sanitize_id(invoice)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        invoice: str,
        params: Optional["InvoiceDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Permanently deletes a one-off invoice draft. This cannot be undone. Attempts to delete invoices that are no longer in a draft state will fail; once an invoice has been finalized or if an invoice is for a subscription, it must be [voided](https://docs.stripe.com/api#void_invoice).
        """
        return cast(
            "Invoice",
            await self._request_async(
                "delete",
                "/v1/invoices/{invoice}".format(invoice=sanitize_id(invoice)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        invoice: str,
        params: Optional["InvoiceRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Retrieves the invoice with the given ID.
        """
        return cast(
            "Invoice",
            self._request(
                "get",
                "/v1/invoices/{invoice}".format(invoice=sanitize_id(invoice)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        invoice: str,
        params: Optional["InvoiceRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Retrieves the invoice with the given ID.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "get",
                "/v1/invoices/{invoice}".format(invoice=sanitize_id(invoice)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        invoice: str,
        params: Optional["InvoiceUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Draft invoices are fully editable. Once an invoice is [finalized](https://docs.stripe.com/docs/billing/invoices/workflow#finalized),
        monetary values, as well as collection_method, become uneditable.

        If you would like to stop the Stripe Billing engine from automatically finalizing, reattempting payments on,
        sending reminders for, or [automatically reconciling](https://docs.stripe.com/docs/billing/invoices/reconciliation) invoices, pass
        auto_advance=false.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}".format(invoice=sanitize_id(invoice)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        invoice: str,
        params: Optional["InvoiceUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Draft invoices are fully editable. Once an invoice is [finalized](https://docs.stripe.com/docs/billing/invoices/workflow#finalized),
        monetary values, as well as collection_method, become uneditable.

        If you would like to stop the Stripe Billing engine from automatically finalizing, reattempting payments on,
        sending reminders for, or [automatically reconciling](https://docs.stripe.com/docs/billing/invoices/reconciliation) invoices, pass
        auto_advance=false.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}".format(invoice=sanitize_id(invoice)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["InvoiceListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Invoice]":
        """
        You can list all invoices, or list the invoices for a specific customer. The invoices are returned sorted by creation date, with the most recently created invoices appearing first.
        """
        return cast(
            "ListObject[Invoice]",
            self._request(
                "get",
                "/v1/invoices",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["InvoiceListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Invoice]":
        """
        You can list all invoices, or list the invoices for a specific customer. The invoices are returned sorted by creation date, with the most recently created invoices appearing first.
        """
        return cast(
            "ListObject[Invoice]",
            await self._request_async(
                "get",
                "/v1/invoices",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["InvoiceCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        This endpoint creates a draft invoice for a given customer. The invoice remains a draft until you [finalize the invoice, which allows you to [pay](https://docs.stripe.com/api#finalize_invoice) or <a href="/api/invoices/send">send](https://docs.stripe.com/api/invoices/pay) the invoice to your customers.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["InvoiceCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        This endpoint creates a draft invoice for a given customer. The invoice remains a draft until you [finalize the invoice, which allows you to [pay](https://docs.stripe.com/api#finalize_invoice) or <a href="/api/invoices/send">send](https://docs.stripe.com/api/invoices/pay) the invoice to your customers.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def search(
        self,
        params: "InvoiceSearchParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SearchResultObject[Invoice]":
        """
        Search for invoices you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cast(
            "SearchResultObject[Invoice]",
            self._request(
                "get",
                "/v1/invoices/search",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def search_async(
        self,
        params: "InvoiceSearchParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SearchResultObject[Invoice]":
        """
        Search for invoices you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cast(
            "SearchResultObject[Invoice]",
            await self._request_async(
                "get",
                "/v1/invoices/search",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def add_lines(
        self,
        invoice: str,
        params: "InvoiceAddLinesParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Adds multiple line items to an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/add_lines".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def add_lines_async(
        self,
        invoice: str,
        params: "InvoiceAddLinesParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Adds multiple line items to an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/add_lines".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def attach_payment(
        self,
        invoice: str,
        params: Optional["InvoiceAttachPaymentParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Attaches a PaymentIntent or an Out of Band Payment to the invoice, adding it to the list of payments.

        For the PaymentIntent, when the PaymentIntent's status changes to succeeded, the payment is credited
        to the invoice, increasing its amount_paid. When the invoice is fully paid, the
        invoice's status becomes paid.

        If the PaymentIntent's status is already succeeded when it's attached, it's
        credited to the invoice immediately.

        See: [Partial payments](https://docs.stripe.com/docs/invoicing/partial-payments) to learn more.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/attach_payment".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def attach_payment_async(
        self,
        invoice: str,
        params: Optional["InvoiceAttachPaymentParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Attaches a PaymentIntent or an Out of Band Payment to the invoice, adding it to the list of payments.

        For the PaymentIntent, when the PaymentIntent's status changes to succeeded, the payment is credited
        to the invoice, increasing its amount_paid. When the invoice is fully paid, the
        invoice's status becomes paid.

        If the PaymentIntent's status is already succeeded when it's attached, it's
        credited to the invoice immediately.

        See: [Partial payments](https://docs.stripe.com/docs/invoicing/partial-payments) to learn more.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/attach_payment".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def finalize_invoice(
        self,
        invoice: str,
        params: Optional["InvoiceFinalizeInvoiceParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Stripe automatically finalizes drafts before sending and attempting payment on invoices. However, if you'd like to finalize a draft invoice manually, you can do so using this method.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/finalize".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def finalize_invoice_async(
        self,
        invoice: str,
        params: Optional["InvoiceFinalizeInvoiceParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Stripe automatically finalizes drafts before sending and attempting payment on invoices. However, if you'd like to finalize a draft invoice manually, you can do so using this method.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/finalize".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def mark_uncollectible(
        self,
        invoice: str,
        params: Optional["InvoiceMarkUncollectibleParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Marking an invoice as uncollectible is useful for keeping track of bad debts that can be written off for accounting purposes.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/mark_uncollectible".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def mark_uncollectible_async(
        self,
        invoice: str,
        params: Optional["InvoiceMarkUncollectibleParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Marking an invoice as uncollectible is useful for keeping track of bad debts that can be written off for accounting purposes.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/mark_uncollectible".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def pay(
        self,
        invoice: str,
        params: Optional["InvoicePayParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Stripe automatically creates and then attempts to collect payment on invoices for customers on subscriptions according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to attempt payment on an invoice out of the normal collection schedule or for some other reason, you can do so.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/pay".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def pay_async(
        self,
        invoice: str,
        params: Optional["InvoicePayParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Stripe automatically creates and then attempts to collect payment on invoices for customers on subscriptions according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to attempt payment on an invoice out of the normal collection schedule or for some other reason, you can do so.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/pay".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def remove_lines(
        self,
        invoice: str,
        params: "InvoiceRemoveLinesParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Removes multiple line items from an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/remove_lines".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def remove_lines_async(
        self,
        invoice: str,
        params: "InvoiceRemoveLinesParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Removes multiple line items from an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/remove_lines".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def send_invoice(
        self,
        invoice: str,
        params: Optional["InvoiceSendInvoiceParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Stripe will automatically send invoices to customers according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to manually send an invoice to your customer out of the normal schedule, you can do so. When sending invoices that have already been paid, there will be no reference to the payment in the email.

        Requests made in test-mode result in no emails being sent, despite sending an invoice.sent event.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/send".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def send_invoice_async(
        self,
        invoice: str,
        params: Optional["InvoiceSendInvoiceParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Stripe will automatically send invoices to customers according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to manually send an invoice to your customer out of the normal schedule, you can do so. When sending invoices that have already been paid, there will be no reference to the payment in the email.

        Requests made in test-mode result in no emails being sent, despite sending an invoice.sent event.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/send".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update_lines(
        self,
        invoice: str,
        params: "InvoiceUpdateLinesParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Updates multiple line items on an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/update_lines".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_lines_async(
        self,
        invoice: str,
        params: "InvoiceUpdateLinesParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Updates multiple line items on an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/update_lines".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def void_invoice(
        self,
        invoice: str,
        params: Optional["InvoiceVoidInvoiceParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Mark a finalized invoice as void. This cannot be undone. Voiding an invoice is similar to [deletion](https://docs.stripe.com/api#delete_invoice), however it only applies to finalized invoices and maintains a papertrail where the invoice can still be found.

        Consult with local regulations to determine whether and how an invoice might be amended, canceled, or voided in the jurisdiction you're doing business in. You might need to [issue another invoice or <a href="#create_credit_note">credit note](https://docs.stripe.com/api#create_invoice) instead. Stripe recommends that you consult with your legal counsel for advice specific to your business.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/void".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def void_invoice_async(
        self,
        invoice: str,
        params: Optional["InvoiceVoidInvoiceParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        Mark a finalized invoice as void. This cannot be undone. Voiding an invoice is similar to [deletion](https://docs.stripe.com/api#delete_invoice), however it only applies to finalized invoices and maintains a papertrail where the invoice can still be found.

        Consult with local regulations to determine whether and how an invoice might be amended, canceled, or voided in the jurisdiction you're doing business in. You might need to [issue another invoice or <a href="#create_credit_note">credit note](https://docs.stripe.com/api#create_invoice) instead. Stripe recommends that you consult with your legal counsel for advice specific to your business.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/void".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create_preview(
        self,
        params: Optional["InvoiceCreatePreviewParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        At any time, you can preview the upcoming invoice for a subscription or subscription schedule. This will show you all the charges that are pending, including subscription renewal charges, invoice item charges, etc. It will also show you any discounts that are applicable to the invoice.

        You can also preview the effects of creating or updating a subscription or subscription schedule, including a preview of any prorations that will take place. To ensure that the actual proration is calculated exactly the same as the previewed proration, you should pass the subscription_details.proration_date parameter when doing the actual subscription update.

        The recommended way to get only the prorations being previewed on the invoice is to consider line items where parent.subscription_item_details.proration is true.

        Note that when you are viewing an upcoming invoice, you are simply viewing a preview – the invoice has not yet been created. As such, the upcoming invoice will not show up in invoice listing calls, and you cannot use the API to pay or edit the invoice. If you want to change the amount that your customer will be billed, you can add, remove, or update pending invoice items, or update the customer's discount.

        Note: Currency conversion calculations use the latest exchange rates. Exchange rates may vary between the time of the preview and the time of the actual invoice creation. [Learn more](https://docs.stripe.com/currencies/conversions)
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/create_preview",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_preview_async(
        self,
        params: Optional["InvoiceCreatePreviewParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Invoice":
        """
        At any time, you can preview the upcoming invoice for a subscription or subscription schedule. This will show you all the charges that are pending, including subscription renewal charges, invoice item charges, etc. It will also show you any discounts that are applicable to the invoice.

        You can also preview the effects of creating or updating a subscription or subscription schedule, including a preview of any prorations that will take place. To ensure that the actual proration is calculated exactly the same as the previewed proration, you should pass the subscription_details.proration_date parameter when doing the actual subscription update.

        The recommended way to get only the prorations being previewed on the invoice is to consider line items where parent.subscription_item_details.proration is true.

        Note that when you are viewing an upcoming invoice, you are simply viewing a preview – the invoice has not yet been created. As such, the upcoming invoice will not show up in invoice listing calls, and you cannot use the API to pay or edit the invoice. If you want to change the amount that your customer will be billed, you can add, remove, or update pending invoice items, or update the customer's discount.

        Note: Currency conversion calculations use the latest exchange rates. Exchange rates may vary between the time of the preview and the time of the actual invoice creation. [Learn more](https://docs.stripe.com/currencies/conversions)
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/create_preview",
                base_address="api",
                params=params,
                options=options,
            ),
        )
