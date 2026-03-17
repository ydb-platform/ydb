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
    from stripe.checkout._session import Session
    from stripe.checkout._session_line_item_service import (
        SessionLineItemService,
    )
    from stripe.params.checkout._session_create_params import (
        SessionCreateParams,
    )
    from stripe.params.checkout._session_expire_params import (
        SessionExpireParams,
    )
    from stripe.params.checkout._session_list_params import SessionListParams
    from stripe.params.checkout._session_retrieve_params import (
        SessionRetrieveParams,
    )
    from stripe.params.checkout._session_update_params import (
        SessionUpdateParams,
    )

_subservices = {
    "line_items": [
        "stripe.checkout._session_line_item_service",
        "SessionLineItemService",
    ],
}


class SessionService(StripeService):
    line_items: "SessionLineItemService"

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
        params: Optional["SessionListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Session]":
        """
        Returns a list of Checkout Sessions.
        """
        return cast(
            "ListObject[Session]",
            self._request(
                "get",
                "/v1/checkout/sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["SessionListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Session]":
        """
        Returns a list of Checkout Sessions.
        """
        return cast(
            "ListObject[Session]",
            await self._request_async(
                "get",
                "/v1/checkout/sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["SessionCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Session":
        """
        Creates a Checkout Session object.
        """
        return cast(
            "Session",
            self._request(
                "post",
                "/v1/checkout/sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["SessionCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Session":
        """
        Creates a Checkout Session object.
        """
        return cast(
            "Session",
            await self._request_async(
                "post",
                "/v1/checkout/sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        session: str,
        params: Optional["SessionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Session":
        """
        Retrieves a Checkout Session object.
        """
        return cast(
            "Session",
            self._request(
                "get",
                "/v1/checkout/sessions/{session}".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        session: str,
        params: Optional["SessionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Session":
        """
        Retrieves a Checkout Session object.
        """
        return cast(
            "Session",
            await self._request_async(
                "get",
                "/v1/checkout/sessions/{session}".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        session: str,
        params: Optional["SessionUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Session":
        """
        Updates a Checkout Session object.

        Related guide: [Dynamically update a Checkout Session](https://docs.stripe.com/payments/advanced/dynamic-updates)
        """
        return cast(
            "Session",
            self._request(
                "post",
                "/v1/checkout/sessions/{session}".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        session: str,
        params: Optional["SessionUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Session":
        """
        Updates a Checkout Session object.

        Related guide: [Dynamically update a Checkout Session](https://docs.stripe.com/payments/advanced/dynamic-updates)
        """
        return cast(
            "Session",
            await self._request_async(
                "post",
                "/v1/checkout/sessions/{session}".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def expire(
        self,
        session: str,
        params: Optional["SessionExpireParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Session":
        """
        A Checkout Session can be expired when it is in one of these statuses: open

        After it expires, a customer can't complete a Checkout Session and customers loading the Checkout Session see a message saying the Checkout Session is expired.
        """
        return cast(
            "Session",
            self._request(
                "post",
                "/v1/checkout/sessions/{session}/expire".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def expire_async(
        self,
        session: str,
        params: Optional["SessionExpireParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Session":
        """
        A Checkout Session can be expired when it is in one of these statuses: open

        After it expires, a customer can't complete a Checkout Session and customers loading the Checkout Session see a message saying the Checkout Session is expired.
        """
        return cast(
            "Session",
            await self._request_async(
                "post",
                "/v1/checkout/sessions/{session}/expire".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
