# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.issuing._transaction import Transaction
    from stripe.params.test_helpers.issuing._transaction_create_force_capture_params import (
        TransactionCreateForceCaptureParams,
    )
    from stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params import (
        TransactionCreateUnlinkedRefundParams,
    )
    from stripe.params.test_helpers.issuing._transaction_refund_params import (
        TransactionRefundParams,
    )


class TransactionService(StripeService):
    def refund(
        self,
        transaction: str,
        params: Optional["TransactionRefundParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Transaction":
        """
        Refund a test-mode Transaction.
        """
        return cast(
            "Transaction",
            self._request(
                "post",
                "/v1/test_helpers/issuing/transactions/{transaction}/refund".format(
                    transaction=sanitize_id(transaction),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def refund_async(
        self,
        transaction: str,
        params: Optional["TransactionRefundParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Transaction":
        """
        Refund a test-mode Transaction.
        """
        return cast(
            "Transaction",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/transactions/{transaction}/refund".format(
                    transaction=sanitize_id(transaction),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create_force_capture(
        self,
        params: "TransactionCreateForceCaptureParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Transaction":
        """
        Allows the user to capture an arbitrary amount, also known as a forced capture.
        """
        return cast(
            "Transaction",
            self._request(
                "post",
                "/v1/test_helpers/issuing/transactions/create_force_capture",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_force_capture_async(
        self,
        params: "TransactionCreateForceCaptureParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Transaction":
        """
        Allows the user to capture an arbitrary amount, also known as a forced capture.
        """
        return cast(
            "Transaction",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/transactions/create_force_capture",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create_unlinked_refund(
        self,
        params: "TransactionCreateUnlinkedRefundParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Transaction":
        """
        Allows the user to refund an arbitrary amount, also known as a unlinked refund.
        """
        return cast(
            "Transaction",
            self._request(
                "post",
                "/v1/test_helpers/issuing/transactions/create_unlinked_refund",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_unlinked_refund_async(
        self,
        params: "TransactionCreateUnlinkedRefundParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Transaction":
        """
        Allows the user to refund an arbitrary amount, also known as a unlinked refund.
        """
        return cast(
            "Transaction",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/transactions/create_unlinked_refund",
                base_address="api",
                params=params,
                options=options,
            ),
        )
