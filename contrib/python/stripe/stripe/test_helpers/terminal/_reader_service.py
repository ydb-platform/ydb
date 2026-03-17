# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.test_helpers.terminal._reader_present_payment_method_params import (
        ReaderPresentPaymentMethodParams,
    )
    from stripe.params.test_helpers.terminal._reader_succeed_input_collection_params import (
        ReaderSucceedInputCollectionParams,
    )
    from stripe.params.test_helpers.terminal._reader_timeout_input_collection_params import (
        ReaderTimeoutInputCollectionParams,
    )
    from stripe.terminal._reader import Reader


class ReaderService(StripeService):
    def present_payment_method(
        self,
        reader: str,
        params: Optional["ReaderPresentPaymentMethodParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Presents a payment method on a simulated reader. Can be used to simulate accepting a payment, saving a card or refunding a transaction.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/test_helpers/terminal/readers/{reader}/present_payment_method".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def present_payment_method_async(
        self,
        reader: str,
        params: Optional["ReaderPresentPaymentMethodParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Presents a payment method on a simulated reader. Can be used to simulate accepting a payment, saving a card or refunding a transaction.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/test_helpers/terminal/readers/{reader}/present_payment_method".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def succeed_input_collection(
        self,
        reader: str,
        params: Optional["ReaderSucceedInputCollectionParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Use this endpoint to trigger a successful input collection on a simulated reader.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/test_helpers/terminal/readers/{reader}/succeed_input_collection".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def succeed_input_collection_async(
        self,
        reader: str,
        params: Optional["ReaderSucceedInputCollectionParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Use this endpoint to trigger a successful input collection on a simulated reader.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/test_helpers/terminal/readers/{reader}/succeed_input_collection".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def timeout_input_collection(
        self,
        reader: str,
        params: Optional["ReaderTimeoutInputCollectionParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Use this endpoint to complete an input collection with a timeout error on a simulated reader.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/test_helpers/terminal/readers/{reader}/timeout_input_collection".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def timeout_input_collection_async(
        self,
        reader: str,
        params: Optional["ReaderTimeoutInputCollectionParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reader":
        """
        Use this endpoint to complete an input collection with a timeout error on a simulated reader.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/test_helpers/terminal/readers/{reader}/timeout_input_collection".format(
                    reader=sanitize_id(reader),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
