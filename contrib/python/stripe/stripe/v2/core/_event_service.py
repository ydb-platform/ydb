# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.v2.core._event_list_params import EventListParams
    from stripe.params.v2.core._event_retrieve_params import (
        EventRetrieveParams,
    )
    from stripe.v2._list_object import ListObject
    from stripe.v2.core._event import Event


class EventService(StripeService):
    def list(
        self,
        params: Optional["EventListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Event]":
        """
        List events, going back up to 30 days.
        """
        return cast(
            "ListObject[Event]",
            self._request(
                "get",
                "/v2/core/events",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["EventListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Event]":
        """
        List events, going back up to 30 days.
        """
        return cast(
            "ListObject[Event]",
            await self._request_async(
                "get",
                "/v2/core/events",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["EventRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Event":
        """
        Retrieves the details of an event.
        """
        return cast(
            "Event",
            self._request(
                "get",
                "/v2/core/events/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["EventRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Event":
        """
        Retrieves the details of an event.
        """
        return cast(
            "Event",
            await self._request_async(
                "get",
                "/v2/core/events/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
