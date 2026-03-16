# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.radar._early_fraud_warning_list_params import (
        EarlyFraudWarningListParams,
    )
    from stripe.params.radar._early_fraud_warning_retrieve_params import (
        EarlyFraudWarningRetrieveParams,
    )
    from stripe.radar._early_fraud_warning import EarlyFraudWarning


class EarlyFraudWarningService(StripeService):
    def list(
        self,
        params: Optional["EarlyFraudWarningListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[EarlyFraudWarning]":
        """
        Returns a list of early fraud warnings.
        """
        return cast(
            "ListObject[EarlyFraudWarning]",
            self._request(
                "get",
                "/v1/radar/early_fraud_warnings",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["EarlyFraudWarningListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[EarlyFraudWarning]":
        """
        Returns a list of early fraud warnings.
        """
        return cast(
            "ListObject[EarlyFraudWarning]",
            await self._request_async(
                "get",
                "/v1/radar/early_fraud_warnings",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        early_fraud_warning: str,
        params: Optional["EarlyFraudWarningRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "EarlyFraudWarning":
        """
        Retrieves the details of an early fraud warning that has previously been created.

        Please refer to the [early fraud warning](https://docs.stripe.com/api#early_fraud_warning_object) object reference for more details.
        """
        return cast(
            "EarlyFraudWarning",
            self._request(
                "get",
                "/v1/radar/early_fraud_warnings/{early_fraud_warning}".format(
                    early_fraud_warning=sanitize_id(early_fraud_warning),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        early_fraud_warning: str,
        params: Optional["EarlyFraudWarningRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "EarlyFraudWarning":
        """
        Retrieves the details of an early fraud warning that has previously been created.

        Please refer to the [early fraud warning](https://docs.stripe.com/api#early_fraud_warning_object) object reference for more details.
        """
        return cast(
            "EarlyFraudWarning",
            await self._request_async(
                "get",
                "/v1/radar/early_fraud_warnings/{early_fraud_warning}".format(
                    early_fraud_warning=sanitize_id(early_fraud_warning),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
