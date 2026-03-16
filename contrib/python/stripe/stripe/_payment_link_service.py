# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._payment_link import PaymentLink
    from stripe._payment_link_line_item_service import (
        PaymentLinkLineItemService,
    )
    from stripe._request_options import RequestOptions
    from stripe.params._payment_link_create_params import (
        PaymentLinkCreateParams,
    )
    from stripe.params._payment_link_list_params import PaymentLinkListParams
    from stripe.params._payment_link_retrieve_params import (
        PaymentLinkRetrieveParams,
    )
    from stripe.params._payment_link_update_params import (
        PaymentLinkUpdateParams,
    )

_subservices = {
    "line_items": [
        "stripe._payment_link_line_item_service",
        "PaymentLinkLineItemService",
    ],
}


class PaymentLinkService(StripeService):
    line_items: "PaymentLinkLineItemService"

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
        params: Optional["PaymentLinkListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PaymentLink]":
        """
        Returns a list of your payment links.
        """
        return cast(
            "ListObject[PaymentLink]",
            self._request(
                "get",
                "/v1/payment_links",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["PaymentLinkListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PaymentLink]":
        """
        Returns a list of your payment links.
        """
        return cast(
            "ListObject[PaymentLink]",
            await self._request_async(
                "get",
                "/v1/payment_links",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "PaymentLinkCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentLink":
        """
        Creates a payment link.
        """
        return cast(
            "PaymentLink",
            self._request(
                "post",
                "/v1/payment_links",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "PaymentLinkCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentLink":
        """
        Creates a payment link.
        """
        return cast(
            "PaymentLink",
            await self._request_async(
                "post",
                "/v1/payment_links",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        payment_link: str,
        params: Optional["PaymentLinkRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentLink":
        """
        Retrieve a payment link.
        """
        return cast(
            "PaymentLink",
            self._request(
                "get",
                "/v1/payment_links/{payment_link}".format(
                    payment_link=sanitize_id(payment_link),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        payment_link: str,
        params: Optional["PaymentLinkRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentLink":
        """
        Retrieve a payment link.
        """
        return cast(
            "PaymentLink",
            await self._request_async(
                "get",
                "/v1/payment_links/{payment_link}".format(
                    payment_link=sanitize_id(payment_link),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        payment_link: str,
        params: Optional["PaymentLinkUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentLink":
        """
        Updates a payment link.
        """
        return cast(
            "PaymentLink",
            self._request(
                "post",
                "/v1/payment_links/{payment_link}".format(
                    payment_link=sanitize_id(payment_link),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        payment_link: str,
        params: Optional["PaymentLinkUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentLink":
        """
        Updates a payment link.
        """
        return cast(
            "PaymentLink",
            await self._request_async(
                "post",
                "/v1/payment_links/{payment_link}".format(
                    payment_link=sanitize_id(payment_link),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
