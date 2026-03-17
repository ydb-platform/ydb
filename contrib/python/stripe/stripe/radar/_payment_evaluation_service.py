# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.radar._payment_evaluation_create_params import (
        PaymentEvaluationCreateParams,
    )
    from stripe.radar._payment_evaluation import PaymentEvaluation


class PaymentEvaluationService(StripeService):
    def create(
        self,
        params: "PaymentEvaluationCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentEvaluation":
        """
        Request a Radar API fraud risk score from Stripe for a payment before sending it for external processor authorization.
        """
        return cast(
            "PaymentEvaluation",
            self._request(
                "post",
                "/v1/radar/payment_evaluations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "PaymentEvaluationCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentEvaluation":
        """
        Request a Radar API fraud risk score from Stripe for a payment before sending it for external processor authorization.
        """
        return cast(
            "PaymentEvaluation",
            await self._request_async(
                "post",
                "/v1/radar/payment_evaluations",
                base_address="api",
                params=params,
                options=options,
            ),
        )
