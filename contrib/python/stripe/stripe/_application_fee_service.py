# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._application_fee import ApplicationFee
    from stripe._application_fee_refund_service import (
        ApplicationFeeRefundService,
    )
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._application_fee_list_params import (
        ApplicationFeeListParams,
    )
    from stripe.params._application_fee_retrieve_params import (
        ApplicationFeeRetrieveParams,
    )

_subservices = {
    "refunds": [
        "stripe._application_fee_refund_service",
        "ApplicationFeeRefundService",
    ],
}


class ApplicationFeeService(StripeService):
    refunds: "ApplicationFeeRefundService"

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
        params: Optional["ApplicationFeeListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ApplicationFee]":
        """
        Returns a list of application fees you've previously collected. The application fees are returned in sorted order, with the most recent fees appearing first.
        """
        return cast(
            "ListObject[ApplicationFee]",
            self._request(
                "get",
                "/v1/application_fees",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["ApplicationFeeListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ApplicationFee]":
        """
        Returns a list of application fees you've previously collected. The application fees are returned in sorted order, with the most recent fees appearing first.
        """
        return cast(
            "ListObject[ApplicationFee]",
            await self._request_async(
                "get",
                "/v1/application_fees",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["ApplicationFeeRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ApplicationFee":
        """
        Retrieves the details of an application fee that your account has collected. The same information is returned when refunding the application fee.
        """
        return cast(
            "ApplicationFee",
            self._request(
                "get",
                "/v1/application_fees/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["ApplicationFeeRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ApplicationFee":
        """
        Retrieves the details of an application fee that your account has collected. The same information is returned when refunding the application fee.
        """
        return cast(
            "ApplicationFee",
            await self._request_async(
                "get",
                "/v1/application_fees/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
