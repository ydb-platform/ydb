# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.billing._meter import Meter
    from stripe.billing._meter_event_summary_service import (
        MeterEventSummaryService,
    )
    from stripe.params.billing._meter_create_params import MeterCreateParams
    from stripe.params.billing._meter_deactivate_params import (
        MeterDeactivateParams,
    )
    from stripe.params.billing._meter_list_params import MeterListParams
    from stripe.params.billing._meter_reactivate_params import (
        MeterReactivateParams,
    )
    from stripe.params.billing._meter_retrieve_params import (
        MeterRetrieveParams,
    )
    from stripe.params.billing._meter_update_params import MeterUpdateParams

_subservices = {
    "event_summaries": [
        "stripe.billing._meter_event_summary_service",
        "MeterEventSummaryService",
    ],
}


class MeterService(StripeService):
    event_summaries: "MeterEventSummaryService"

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
        params: Optional["MeterListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Meter]":
        """
        Retrieve a list of billing meters.
        """
        return cast(
            "ListObject[Meter]",
            self._request(
                "get",
                "/v1/billing/meters",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["MeterListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Meter]":
        """
        Retrieve a list of billing meters.
        """
        return cast(
            "ListObject[Meter]",
            await self._request_async(
                "get",
                "/v1/billing/meters",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "MeterCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Meter":
        """
        Creates a billing meter.
        """
        return cast(
            "Meter",
            self._request(
                "post",
                "/v1/billing/meters",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "MeterCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Meter":
        """
        Creates a billing meter.
        """
        return cast(
            "Meter",
            await self._request_async(
                "post",
                "/v1/billing/meters",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["MeterRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Meter":
        """
        Retrieves a billing meter given an ID.
        """
        return cast(
            "Meter",
            self._request(
                "get",
                "/v1/billing/meters/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["MeterRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Meter":
        """
        Retrieves a billing meter given an ID.
        """
        return cast(
            "Meter",
            await self._request_async(
                "get",
                "/v1/billing/meters/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        id: str,
        params: Optional["MeterUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Meter":
        """
        Updates a billing meter.
        """
        return cast(
            "Meter",
            self._request(
                "post",
                "/v1/billing/meters/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        id: str,
        params: Optional["MeterUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Meter":
        """
        Updates a billing meter.
        """
        return cast(
            "Meter",
            await self._request_async(
                "post",
                "/v1/billing/meters/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def deactivate(
        self,
        id: str,
        params: Optional["MeterDeactivateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Meter":
        """
        When a meter is deactivated, no more meter events will be accepted for this meter. You can't attach a deactivated meter to a price.
        """
        return cast(
            "Meter",
            self._request(
                "post",
                "/v1/billing/meters/{id}/deactivate".format(
                    id=sanitize_id(id)
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def deactivate_async(
        self,
        id: str,
        params: Optional["MeterDeactivateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Meter":
        """
        When a meter is deactivated, no more meter events will be accepted for this meter. You can't attach a deactivated meter to a price.
        """
        return cast(
            "Meter",
            await self._request_async(
                "post",
                "/v1/billing/meters/{id}/deactivate".format(
                    id=sanitize_id(id)
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def reactivate(
        self,
        id: str,
        params: Optional["MeterReactivateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Meter":
        """
        When a meter is reactivated, events for this meter can be accepted and you can attach the meter to a price.
        """
        return cast(
            "Meter",
            self._request(
                "post",
                "/v1/billing/meters/{id}/reactivate".format(
                    id=sanitize_id(id)
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def reactivate_async(
        self,
        id: str,
        params: Optional["MeterReactivateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Meter":
        """
        When a meter is reactivated, events for this meter can be accepted and you can attach the meter to a price.
        """
        return cast(
            "Meter",
            await self._request_async(
                "post",
                "/v1/billing/meters/{id}/reactivate".format(
                    id=sanitize_id(id)
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
