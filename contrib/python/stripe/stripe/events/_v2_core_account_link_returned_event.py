# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._api_mode import ApiMode
from stripe._stripe_object import StripeObject
from stripe._stripe_response import StripeResponse
from stripe.v2.core._event import Event, EventNotification
from typing import Any, Dict, List, Optional, cast
from typing_extensions import Literal, TYPE_CHECKING, override

if TYPE_CHECKING:
    from stripe._api_requestor import _APIRequestor


class V2CoreAccountLinkReturnedEventNotification(EventNotification):
    LOOKUP_TYPE = "v2.core.account_link.returned"
    type: Literal["v2.core.account_link.returned"]

    @override
    def fetch_event(self) -> "V2CoreAccountLinkReturnedEvent":
        return cast(
            "V2CoreAccountLinkReturnedEvent",
            super().fetch_event(),
        )

    @override
    async def fetch_event_async(self) -> "V2CoreAccountLinkReturnedEvent":
        return cast(
            "V2CoreAccountLinkReturnedEvent",
            await super().fetch_event_async(),
        )


class V2CoreAccountLinkReturnedEvent(Event):
    LOOKUP_TYPE = "v2.core.account_link.returned"
    type: Literal["v2.core.account_link.returned"]

    class V2CoreAccountLinkReturnedEventData(StripeObject):
        account_id: str
        """
        The ID of the v2 account.
        """
        configurations: List[Literal["customer", "merchant", "recipient"]]
        """
        Configurations on the Account that was onboarded via the account link.
        """
        use_case: Literal["account_onboarding", "account_update"]
        """
        Open Enum. The use case type of the account link that has been completed.
        """

    data: V2CoreAccountLinkReturnedEventData
    """
    Data for the v2.core.account_link.returned event
    """

    @classmethod
    def _construct_from(
        cls,
        *,
        values: Dict[str, Any],
        last_response: Optional[StripeResponse] = None,
        requestor: "_APIRequestor",
        api_mode: ApiMode,
    ) -> "V2CoreAccountLinkReturnedEvent":
        evt = super()._construct_from(
            values=values,
            last_response=last_response,
            requestor=requestor,
            api_mode=api_mode,
        )
        if hasattr(evt, "data"):
            evt.data = V2CoreAccountLinkReturnedEvent.V2CoreAccountLinkReturnedEventData._construct_from(
                values=evt.data,
                last_response=last_response,
                requestor=requestor,
                api_mode=api_mode,
            )
        return evt
