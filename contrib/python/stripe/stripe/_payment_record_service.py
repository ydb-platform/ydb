# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._payment_record import PaymentRecord
    from stripe._request_options import RequestOptions
    from stripe.params._payment_record_report_payment_attempt_canceled_params import (
        PaymentRecordReportPaymentAttemptCanceledParams,
    )
    from stripe.params._payment_record_report_payment_attempt_failed_params import (
        PaymentRecordReportPaymentAttemptFailedParams,
    )
    from stripe.params._payment_record_report_payment_attempt_guaranteed_params import (
        PaymentRecordReportPaymentAttemptGuaranteedParams,
    )
    from stripe.params._payment_record_report_payment_attempt_informational_params import (
        PaymentRecordReportPaymentAttemptInformationalParams,
    )
    from stripe.params._payment_record_report_payment_attempt_params import (
        PaymentRecordReportPaymentAttemptParams,
    )
    from stripe.params._payment_record_report_payment_params import (
        PaymentRecordReportPaymentParams,
    )
    from stripe.params._payment_record_report_refund_params import (
        PaymentRecordReportRefundParams,
    )
    from stripe.params._payment_record_retrieve_params import (
        PaymentRecordRetrieveParams,
    )


class PaymentRecordService(StripeService):
    def retrieve(
        self,
        id: str,
        params: Optional["PaymentRecordRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Retrieves a Payment Record with the given ID
        """
        return cast(
            "PaymentRecord",
            self._request(
                "get",
                "/v1/payment_records/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["PaymentRecordRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Retrieves a Payment Record with the given ID
        """
        return cast(
            "PaymentRecord",
            await self._request_async(
                "get",
                "/v1/payment_records/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def report_payment_attempt(
        self,
        id: str,
        params: "PaymentRecordReportPaymentAttemptParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Report a new payment attempt on the specified Payment Record. A new payment
         attempt can only be specified if all other payment attempts are canceled or failed.
        """
        return cast(
            "PaymentRecord",
            self._request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def report_payment_attempt_async(
        self,
        id: str,
        params: "PaymentRecordReportPaymentAttemptParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Report a new payment attempt on the specified Payment Record. A new payment
         attempt can only be specified if all other payment attempts are canceled or failed.
        """
        return cast(
            "PaymentRecord",
            await self._request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def report_payment_attempt_canceled(
        self,
        id: str,
        params: "PaymentRecordReportPaymentAttemptCanceledParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was canceled.
        """
        return cast(
            "PaymentRecord",
            self._request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_canceled".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def report_payment_attempt_canceled_async(
        self,
        id: str,
        params: "PaymentRecordReportPaymentAttemptCanceledParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was canceled.
        """
        return cast(
            "PaymentRecord",
            await self._request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_canceled".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def report_payment_attempt_failed(
        self,
        id: str,
        params: "PaymentRecordReportPaymentAttemptFailedParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         failed or errored.
        """
        return cast(
            "PaymentRecord",
            self._request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_failed".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def report_payment_attempt_failed_async(
        self,
        id: str,
        params: "PaymentRecordReportPaymentAttemptFailedParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         failed or errored.
        """
        return cast(
            "PaymentRecord",
            await self._request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_failed".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def report_payment_attempt_guaranteed(
        self,
        id: str,
        params: "PaymentRecordReportPaymentAttemptGuaranteedParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was guaranteed.
        """
        return cast(
            "PaymentRecord",
            self._request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_guaranteed".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def report_payment_attempt_guaranteed_async(
        self,
        id: str,
        params: "PaymentRecordReportPaymentAttemptGuaranteedParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was guaranteed.
        """
        return cast(
            "PaymentRecord",
            await self._request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_guaranteed".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def report_payment_attempt_informational(
        self,
        id: str,
        params: Optional[
            "PaymentRecordReportPaymentAttemptInformationalParams"
        ] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Report informational updates on the specified Payment Record.
        """
        return cast(
            "PaymentRecord",
            self._request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_informational".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def report_payment_attempt_informational_async(
        self,
        id: str,
        params: Optional[
            "PaymentRecordReportPaymentAttemptInformationalParams"
        ] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Report informational updates on the specified Payment Record.
        """
        return cast(
            "PaymentRecord",
            await self._request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_informational".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def report_refund(
        self,
        id: str,
        params: "PaymentRecordReportRefundParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was refunded.
        """
        return cast(
            "PaymentRecord",
            self._request(
                "post",
                "/v1/payment_records/{id}/report_refund".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def report_refund_async(
        self,
        id: str,
        params: "PaymentRecordReportRefundParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was refunded.
        """
        return cast(
            "PaymentRecord",
            await self._request_async(
                "post",
                "/v1/payment_records/{id}/report_refund".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def report_payment(
        self,
        params: "PaymentRecordReportPaymentParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Report a new Payment Record. You may report a Payment Record as it is
         initialized and later report updates through the other report_* methods, or report Payment
         Records in a terminal state directly, through this method.
        """
        return cast(
            "PaymentRecord",
            self._request(
                "post",
                "/v1/payment_records/report_payment",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def report_payment_async(
        self,
        params: "PaymentRecordReportPaymentParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentRecord":
        """
        Report a new Payment Record. You may report a Payment Record as it is
         initialized and later report updates through the other report_* methods, or report Payment
         Records in a terminal state directly, through this method.
        """
        return cast(
            "PaymentRecord",
            await self._request_async(
                "post",
                "/v1/payment_records/report_payment",
                base_address="api",
                params=params,
                options=options,
            ),
        )
