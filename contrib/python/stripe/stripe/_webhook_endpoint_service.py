# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._webhook_endpoint import WebhookEndpoint
    from stripe.params._webhook_endpoint_create_params import (
        WebhookEndpointCreateParams,
    )
    from stripe.params._webhook_endpoint_delete_params import (
        WebhookEndpointDeleteParams,
    )
    from stripe.params._webhook_endpoint_list_params import (
        WebhookEndpointListParams,
    )
    from stripe.params._webhook_endpoint_retrieve_params import (
        WebhookEndpointRetrieveParams,
    )
    from stripe.params._webhook_endpoint_update_params import (
        WebhookEndpointUpdateParams,
    )


class WebhookEndpointService(StripeService):
    def delete(
        self,
        webhook_endpoint: str,
        params: Optional["WebhookEndpointDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "WebhookEndpoint":
        """
        You can also delete webhook endpoints via the [webhook endpoint management](https://dashboard.stripe.com/account/webhooks) page of the Stripe dashboard.
        """
        return cast(
            "WebhookEndpoint",
            self._request(
                "delete",
                "/v1/webhook_endpoints/{webhook_endpoint}".format(
                    webhook_endpoint=sanitize_id(webhook_endpoint),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        webhook_endpoint: str,
        params: Optional["WebhookEndpointDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "WebhookEndpoint":
        """
        You can also delete webhook endpoints via the [webhook endpoint management](https://dashboard.stripe.com/account/webhooks) page of the Stripe dashboard.
        """
        return cast(
            "WebhookEndpoint",
            await self._request_async(
                "delete",
                "/v1/webhook_endpoints/{webhook_endpoint}".format(
                    webhook_endpoint=sanitize_id(webhook_endpoint),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        webhook_endpoint: str,
        params: Optional["WebhookEndpointRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "WebhookEndpoint":
        """
        Retrieves the webhook endpoint with the given ID.
        """
        return cast(
            "WebhookEndpoint",
            self._request(
                "get",
                "/v1/webhook_endpoints/{webhook_endpoint}".format(
                    webhook_endpoint=sanitize_id(webhook_endpoint),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        webhook_endpoint: str,
        params: Optional["WebhookEndpointRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "WebhookEndpoint":
        """
        Retrieves the webhook endpoint with the given ID.
        """
        return cast(
            "WebhookEndpoint",
            await self._request_async(
                "get",
                "/v1/webhook_endpoints/{webhook_endpoint}".format(
                    webhook_endpoint=sanitize_id(webhook_endpoint),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        webhook_endpoint: str,
        params: Optional["WebhookEndpointUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "WebhookEndpoint":
        """
        Updates the webhook endpoint. You may edit the url, the list of enabled_events, and the status of your endpoint.
        """
        return cast(
            "WebhookEndpoint",
            self._request(
                "post",
                "/v1/webhook_endpoints/{webhook_endpoint}".format(
                    webhook_endpoint=sanitize_id(webhook_endpoint),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        webhook_endpoint: str,
        params: Optional["WebhookEndpointUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "WebhookEndpoint":
        """
        Updates the webhook endpoint. You may edit the url, the list of enabled_events, and the status of your endpoint.
        """
        return cast(
            "WebhookEndpoint",
            await self._request_async(
                "post",
                "/v1/webhook_endpoints/{webhook_endpoint}".format(
                    webhook_endpoint=sanitize_id(webhook_endpoint),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["WebhookEndpointListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[WebhookEndpoint]":
        """
        Returns a list of your webhook endpoints.
        """
        return cast(
            "ListObject[WebhookEndpoint]",
            self._request(
                "get",
                "/v1/webhook_endpoints",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["WebhookEndpointListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[WebhookEndpoint]":
        """
        Returns a list of your webhook endpoints.
        """
        return cast(
            "ListObject[WebhookEndpoint]",
            await self._request_async(
                "get",
                "/v1/webhook_endpoints",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "WebhookEndpointCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "WebhookEndpoint":
        """
        A webhook endpoint must have a url and a list of enabled_events. You may optionally specify the Boolean connect parameter. If set to true, then a Connect webhook endpoint that notifies the specified url about events from all connected accounts is created; otherwise an account webhook endpoint that notifies the specified url only about events from your account is created. You can also create webhook endpoints in the [webhooks settings](https://dashboard.stripe.com/account/webhooks) section of the Dashboard.
        """
        return cast(
            "WebhookEndpoint",
            self._request(
                "post",
                "/v1/webhook_endpoints",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "WebhookEndpointCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "WebhookEndpoint":
        """
        A webhook endpoint must have a url and a list of enabled_events. You may optionally specify the Boolean connect parameter. If set to true, then a Connect webhook endpoint that notifies the specified url about events from all connected accounts is created; otherwise an account webhook endpoint that notifies the specified url only about events from your account is created. You can also create webhook endpoints in the [webhooks settings](https://dashboard.stripe.com/account/webhooks) section of the Dashboard.
        """
        return cast(
            "WebhookEndpoint",
            await self._request_async(
                "post",
                "/v1/webhook_endpoints",
                base_address="api",
                params=params,
                options=options,
            ),
        )
