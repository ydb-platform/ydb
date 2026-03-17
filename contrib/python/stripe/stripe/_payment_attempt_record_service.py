# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._payment_attempt_record import PaymentAttemptRecord
    from stripe._request_options import RequestOptions
    from stripe.params._payment_attempt_record_list_params import (
        PaymentAttemptRecordListParams,
    )
    from stripe.params._payment_attempt_record_retrieve_params import (
        PaymentAttemptRecordRetrieveParams,
    )


class PaymentAttemptRecordService(StripeService):
    def list(
        self,
        params: "PaymentAttemptRecordListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PaymentAttemptRecord]":
        """
        List all the Payment Attempt Records attached to the specified Payment Record.
        """
        return cast(
            "ListObject[PaymentAttemptRecord]",
            self._request(
                "get",
                "/v1/payment_attempt_records",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "PaymentAttemptRecordListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PaymentAttemptRecord]":
        """
        List all the Payment Attempt Records attached to the specified Payment Record.
        """
        return cast(
            "ListObject[PaymentAttemptRecord]",
            await self._request_async(
                "get",
                "/v1/payment_attempt_records",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["PaymentAttemptRecordRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentAttemptRecord":
        """
        Retrieves a Payment Attempt Record with the given ID
        """
        return cast(
            "PaymentAttemptRecord",
            self._request(
                "get",
                "/v1/payment_attempt_records/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["PaymentAttemptRecordRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentAttemptRecord":
        """
        Retrieves a Payment Attempt Record with the given ID
        """
        return cast(
            "PaymentAttemptRecord",
            await self._request_async(
                "get",
                "/v1/payment_attempt_records/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
