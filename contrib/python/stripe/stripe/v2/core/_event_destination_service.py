# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.v2.core._event_destination_create_params import (
        EventDestinationCreateParams,
    )
    from stripe.params.v2.core._event_destination_delete_params import (
        EventDestinationDeleteParams,
    )
    from stripe.params.v2.core._event_destination_disable_params import (
        EventDestinationDisableParams,
    )
    from stripe.params.v2.core._event_destination_enable_params import (
        EventDestinationEnableParams,
    )
    from stripe.params.v2.core._event_destination_list_params import (
        EventDestinationListParams,
    )
    from stripe.params.v2.core._event_destination_ping_params import (
        EventDestinationPingParams,
    )
    from stripe.params.v2.core._event_destination_retrieve_params import (
        EventDestinationRetrieveParams,
    )
    from stripe.params.v2.core._event_destination_update_params import (
        EventDestinationUpdateParams,
    )
    from stripe.v2._deleted_object import DeletedObject
    from stripe.v2._list_object import ListObject
    from stripe.v2.core._event import Event
    from stripe.v2.core._event_destination import EventDestination


class EventDestinationService(StripeService):
    def list(
        self,
        params: Optional["EventDestinationListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[EventDestination]":
        """
        Lists all event destinations.
        """
        return cast(
            "ListObject[EventDestination]",
            self._request(
                "get",
                "/v2/core/event_destinations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["EventDestinationListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[EventDestination]":
        """
        Lists all event destinations.
        """
        return cast(
            "ListObject[EventDestination]",
            await self._request_async(
                "get",
                "/v2/core/event_destinations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "EventDestinationCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "EventDestination":
        """
        Create a new event destination.
        """
        return cast(
            "EventDestination",
            self._request(
                "post",
                "/v2/core/event_destinations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "EventDestinationCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "EventDestination":
        """
        Create a new event destination.
        """
        return cast(
            "EventDestination",
            await self._request_async(
                "post",
                "/v2/core/event_destinations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def delete(
        self,
        id: str,
        params: Optional["EventDestinationDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "DeletedObject":
        """
        Delete an event destination.
        """
        return cast(
            "DeletedObject",
            self._request(
                "delete",
                "/v2/core/event_destinations/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        id: str,
        params: Optional["EventDestinationDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "DeletedObject":
        """
        Delete an event destination.
        """
        return cast(
            "DeletedObject",
            await self._request_async(
                "delete",
                "/v2/core/event_destinations/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["EventDestinationRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "EventDestination":
        """
        Retrieves the details of an event destination.
        """
        return cast(
            "EventDestination",
            self._request(
                "get",
                "/v2/core/event_destinations/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["EventDestinationRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "EventDestination":
        """
        Retrieves the details of an event destination.
        """
        return cast(
            "EventDestination",
            await self._request_async(
                "get",
                "/v2/core/event_destinations/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        id: str,
        params: Optional["EventDestinationUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "EventDestination":
        """
        Update the details of an event destination.
        """
        return cast(
            "EventDestination",
            self._request(
                "post",
                "/v2/core/event_destinations/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        id: str,
        params: Optional["EventDestinationUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "EventDestination":
        """
        Update the details of an event destination.
        """
        return cast(
            "EventDestination",
            await self._request_async(
                "post",
                "/v2/core/event_destinations/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def disable(
        self,
        id: str,
        params: Optional["EventDestinationDisableParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "EventDestination":
        """
        Disable an event destination.
        """
        return cast(
            "EventDestination",
            self._request(
                "post",
                "/v2/core/event_destinations/{id}/disable".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def disable_async(
        self,
        id: str,
        params: Optional["EventDestinationDisableParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "EventDestination":
        """
        Disable an event destination.
        """
        return cast(
            "EventDestination",
            await self._request_async(
                "post",
                "/v2/core/event_destinations/{id}/disable".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def enable(
        self,
        id: str,
        params: Optional["EventDestinationEnableParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "EventDestination":
        """
        Enable an event destination.
        """
        return cast(
            "EventDestination",
            self._request(
                "post",
                "/v2/core/event_destinations/{id}/enable".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def enable_async(
        self,
        id: str,
        params: Optional["EventDestinationEnableParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "EventDestination":
        """
        Enable an event destination.
        """
        return cast(
            "EventDestination",
            await self._request_async(
                "post",
                "/v2/core/event_destinations/{id}/enable".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def ping(
        self,
        id: str,
        params: Optional["EventDestinationPingParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Event":
        """
        Send a `ping` event to an event destination.
        """
        return cast(
            "Event",
            self._request(
                "post",
                "/v2/core/event_destinations/{id}/ping".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def ping_async(
        self,
        id: str,
        params: Optional["EventDestinationPingParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Event":
        """
        Send a `ping` event to an event destination.
        """
        return cast(
            "Event",
            await self._request_async(
                "post",
                "/v2/core/event_destinations/{id}/ping".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
