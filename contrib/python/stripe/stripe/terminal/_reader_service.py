# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.terminal._reader_cancel_action_params import (
        ReaderCancelActionParams,
    )
    from stripe.params.terminal._reader_collect_inputs_params import (
        ReaderCollectInputsParams,
    )
    from stripe.params.terminal._reader_collect_payment_method_params import (
        ReaderCollectPaymentMethodParams,
    )
    from stripe.params.terminal._reader_confirm_payment_intent_params import (
        ReaderConfirmPaymentIntentParams,
    )
    from stripe.params.terminal._reader_create_params import ReaderCreateParams
    from stripe.params.terminal._reader_delete_params import ReaderDeleteParams
    from stripe.params.terminal._reader_list_params import ReaderListParams
    from stripe.params.terminal._reader_process_payment_intent_params import (
        ReaderProcessPaymentIntentParams,
    )
    from stripe.params.terminal._reader_process_setup_intent_params import (
        ReaderProcessSetupIntentParams,
    )
    from stripe.params.terminal._reader_refund_payment_params import (
        ReaderRefundPaymentParams,
    )
    from stripe.params.terminal._reader_retrieve_params import (
        ReaderRetrieveParams,
    )
    from stripe.params.terminal._reader_set_reader_display_params import (
        ReaderSetReaderDisplayParams,
    )
    from stripe.params.terminal._reader_update_params import ReaderUpdateParams
    from stripe.terminal._reader import Reader


class ReaderService(StripeService):
    def delete(
        self,
        reader: str,
        params: Optional["ReaderDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Deletes a Reader object.
        """
        return cast(
            "Reader",
            self._request(
                "delete",
                "/v1/terminal/readers/{reader}".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        reader: str,
        params: Optional["ReaderDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Deletes a Reader object.
        """
        return cast(
            "Reader",
            await self._request_async(
                "delete",
                "/v1/terminal/readers/{reader}".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        reader: str,
        params: Optional["ReaderRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Retrieves a Reader object.
        """
        return cast(
            "Reader",
            self._request(
                "get",
                "/v1/terminal/readers/{reader}".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        reader: str,
        params: Optional["ReaderRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Retrieves a Reader object.
        """
        return cast(
            "Reader",
            await self._request_async(
                "get",
                "/v1/terminal/readers/{reader}".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        reader: str,
        params: Optional["ReaderUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Updates a Reader object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        reader: str,
        params: Optional["ReaderUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Updates a Reader object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["ReaderListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Reader]":
        """
        Returns a list of Reader objects.
        """
        return cast(
            "ListObject[Reader]",
            self._request(
                "get",
                "/v1/terminal/readers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["ReaderListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Reader]":
        """
        Returns a list of Reader objects.
        """
        return cast(
            "ListObject[Reader]",
            await self._request_async(
                "get",
                "/v1/terminal/readers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "ReaderCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Creates a new Reader object.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "ReaderCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Creates a new Reader object.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def cancel_action(
        self,
        reader: str,
        params: Optional["ReaderCancelActionParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Cancels the current reader action. See [Programmatic Cancellation](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven#programmatic-cancellation) for more details.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/cancel_action".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def cancel_action_async(
        self,
        reader: str,
        params: Optional["ReaderCancelActionParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Cancels the current reader action. See [Programmatic Cancellation](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven#programmatic-cancellation) for more details.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/cancel_action".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def collect_inputs(
        self,
        reader: str,
        params: "ReaderCollectInputsParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Initiates an [input collection flow](https://docs.stripe.com/docs/terminal/features/collect-inputs) on a Reader to display input forms and collect information from your customers.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/collect_inputs".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def collect_inputs_async(
        self,
        reader: str,
        params: "ReaderCollectInputsParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Initiates an [input collection flow](https://docs.stripe.com/docs/terminal/features/collect-inputs) on a Reader to display input forms and collect information from your customers.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/collect_inputs".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def collect_payment_method(
        self,
        reader: str,
        params: "ReaderCollectPaymentMethodParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader and updates the PaymentIntent with card details before manual confirmation. See [Collecting a Payment method](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#collect-a-paymentmethod) for more details.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/collect_payment_method".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def collect_payment_method_async(
        self,
        reader: str,
        params: "ReaderCollectPaymentMethodParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader and updates the PaymentIntent with card details before manual confirmation. See [Collecting a Payment method](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#collect-a-paymentmethod) for more details.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/collect_payment_method".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def confirm_payment_intent(
        self,
        reader: str,
        params: "ReaderConfirmPaymentIntentParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Finalizes a payment on a Reader. See [Confirming a Payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#confirm-the-paymentintent) for more details.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/confirm_payment_intent".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def confirm_payment_intent_async(
        self,
        reader: str,
        params: "ReaderConfirmPaymentIntentParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Finalizes a payment on a Reader. See [Confirming a Payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#confirm-the-paymentintent) for more details.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/confirm_payment_intent".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def process_payment_intent(
        self,
        reader: str,
        params: "ReaderProcessPaymentIntentParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader. See [process the payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=immediately#process-payment) for more details.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/process_payment_intent".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def process_payment_intent_async(
        self,
        reader: str,
        params: "ReaderProcessPaymentIntentParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader. See [process the payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=immediately#process-payment) for more details.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/process_payment_intent".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def process_setup_intent(
        self,
        reader: str,
        params: "ReaderProcessSetupIntentParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Initiates a SetupIntent flow on a Reader. See [Save directly without charging](https://docs.stripe.com/docs/terminal/features/saving-payment-details/save-directly) for more details.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/process_setup_intent".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def process_setup_intent_async(
        self,
        reader: str,
        params: "ReaderProcessSetupIntentParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Initiates a SetupIntent flow on a Reader. See [Save directly without charging](https://docs.stripe.com/docs/terminal/features/saving-payment-details/save-directly) for more details.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/process_setup_intent".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def refund_payment(
        self,
        reader: str,
        params: Optional["ReaderRefundPaymentParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Initiates an in-person refund on a Reader. See [Refund an Interac Payment](https://docs.stripe.com/docs/terminal/payments/regional?integration-country=CA#refund-an-interac-payment) for more details.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/refund_payment".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def refund_payment_async(
        self,
        reader: str,
        params: Optional["ReaderRefundPaymentParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Initiates an in-person refund on a Reader. See [Refund an Interac Payment](https://docs.stripe.com/docs/terminal/payments/regional?integration-country=CA#refund-an-interac-payment) for more details.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/refund_payment".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def set_reader_display(
        self,
        reader: str,
        params: "ReaderSetReaderDisplayParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Sets the reader display to show [cart details](https://docs.stripe.com/docs/terminal/features/display).
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/set_reader_display".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def set_reader_display_async(
        self,
        reader: str,
        params: "ReaderSetReaderDisplayParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Sets the reader display to show [cart details](https://docs.stripe.com/docs/terminal/features/display).
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/set_reader_display".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
