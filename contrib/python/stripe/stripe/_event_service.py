# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._event import Event
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._event_list_params import EventListParams
    from stripe.params._event_retrieve_params import EventRetrieveParams


class EventService(StripeService):
    def list(
        self,
        params: Optional["EventListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Event]":
        """
        List events, going back up to 30 days. Each event data is rendered according to Stripe API version at its creation time, specified in [event object](https://docs.stripe.com/api/events/object) api_version attribute (not according to your current Stripe API version or Stripe-Version header).
        """
        return cast(
            "ListObject[Event]",
            self._request(
                "get",
                "/v1/events",
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
        List events, going back up to 30 days. Each event data is rendered according to Stripe API version at its creation time, specified in [event object](https://docs.stripe.com/api/events/object) api_version attribute (not according to your current Stripe API version or Stripe-Version header).
        """
        return cast(
            "ListObject[Event]",
            await self._request_async(
                "get",
                "/v1/events",
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
        Retrieves the details of an event if it was created in the last 30 days. Supply the unique identifier of the event, which you might have received in a webhook.
        """
        return cast(
            "Event",
            self._request(
                "get",
                "/v1/events/{id}".format(id=sanitize_id(id)),
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
        Retrieves the details of an event if it was created in the last 30 days. Supply the unique identifier of the event, which you might have received in a webhook.
        """
        return cast(
            "Event",
            await self._request_async(
                "get",
                "/v1/events/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
