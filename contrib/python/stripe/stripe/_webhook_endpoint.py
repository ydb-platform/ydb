# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params._webhook_endpoint_create_params import (
        WebhookEndpointCreateParams,
    )
    from stripe.params._webhook_endpoint_delete_params import (
        WebhookEndpointDeleteParams,
    )
    from stripe.params._webhook_endpoint_list_params import (
        WebhookEndpointListParams,
    )
    from stripe.params._webhook_endpoint_modify_params import (
        WebhookEndpointModifyParams,
    )
    from stripe.params._webhook_endpoint_retrieve_params import (
        WebhookEndpointRetrieveParams,
    )


class WebhookEndpoint(
    CreateableAPIResource["WebhookEndpoint"],
    DeletableAPIResource["WebhookEndpoint"],
    ListableAPIResource["WebhookEndpoint"],
    UpdateableAPIResource["WebhookEndpoint"],
):
    """
    You can configure [webhook endpoints](https://docs.stripe.com/webhooks/) via the API to be
    notified about events that happen in your Stripe account or connected
    accounts.

    Most users configure webhooks from [the dashboard](https://dashboard.stripe.com/webhooks), which provides a user interface for registering and testing your webhook endpoints.

    Related guide: [Setting up webhooks](https://docs.stripe.com/webhooks/configure)
    """

    OBJECT_NAME: ClassVar[Literal["webhook_endpoint"]] = "webhook_endpoint"
    api_version: Optional[str]
    """
    The API version events are rendered as for this webhook endpoint.
    """
    application: Optional[str]
    """
    The ID of the associated Connect application.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    description: Optional[str]
    """
    An optional description of what the webhook is used for.
    """
    enabled_events: List[str]
    """
    The list of events to enable for this endpoint. `['*']` indicates that all events are enabled, except those that require explicit selection.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["webhook_endpoint"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    secret: Optional[str]
    """
    The endpoint's secret, used to generate [webhook signatures](https://docs.stripe.com/webhooks/signatures). Only returned at creation.
    """
    status: str
    """
    The status of the webhook. It can be `enabled` or `disabled`.
    """
    url: str
    """
    The URL of the webhook endpoint.
    """

    @classmethod
    def create(
        cls, **params: Unpack["WebhookEndpointCreateParams"]
    ) -> "WebhookEndpoint":
        """
        A webhook endpoint must have a url and a list of enabled_events. You may optionally specify the Boolean connect parameter. If set to true, then a Connect webhook endpoint that notifies the specified url about events from all connected accounts is created; otherwise an account webhook endpoint that notifies the specified url only about events from your account is created. You can also create webhook endpoints in the [webhooks settings](https://dashboard.stripe.com/account/webhooks) section of the Dashboard.
        """
        return cast(
            "WebhookEndpoint",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["WebhookEndpointCreateParams"]
    ) -> "WebhookEndpoint":
        """
        A webhook endpoint must have a url and a list of enabled_events. You may optionally specify the Boolean connect parameter. If set to true, then a Connect webhook endpoint that notifies the specified url about events from all connected accounts is created; otherwise an account webhook endpoint that notifies the specified url only about events from your account is created. You can also create webhook endpoints in the [webhooks settings](https://dashboard.stripe.com/account/webhooks) section of the Dashboard.
        """
        return cast(
            "WebhookEndpoint",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["WebhookEndpointDeleteParams"]
    ) -> "WebhookEndpoint":
        """
        You can also delete webhook endpoints via the [webhook endpoint management](https://dashboard.stripe.com/account/webhooks) page of the Stripe dashboard.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "WebhookEndpoint",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(
        sid: str, **params: Unpack["WebhookEndpointDeleteParams"]
    ) -> "WebhookEndpoint":
        """
        You can also delete webhook endpoints via the [webhook endpoint management](https://dashboard.stripe.com/account/webhooks) page of the Stripe dashboard.
        """
        ...

    @overload
    def delete(
        self, **params: Unpack["WebhookEndpointDeleteParams"]
    ) -> "WebhookEndpoint":
        """
        You can also delete webhook endpoints via the [webhook endpoint management](https://dashboard.stripe.com/account/webhooks) page of the Stripe dashboard.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["WebhookEndpointDeleteParams"]
    ) -> "WebhookEndpoint":
        """
        You can also delete webhook endpoints via the [webhook endpoint management](https://dashboard.stripe.com/account/webhooks) page of the Stripe dashboard.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["WebhookEndpointDeleteParams"]
    ) -> "WebhookEndpoint":
        """
        You can also delete webhook endpoints via the [webhook endpoint management](https://dashboard.stripe.com/account/webhooks) page of the Stripe dashboard.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "WebhookEndpoint",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["WebhookEndpointDeleteParams"]
    ) -> "WebhookEndpoint":
        """
        You can also delete webhook endpoints via the [webhook endpoint management](https://dashboard.stripe.com/account/webhooks) page of the Stripe dashboard.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["WebhookEndpointDeleteParams"]
    ) -> "WebhookEndpoint":
        """
        You can also delete webhook endpoints via the [webhook endpoint management](https://dashboard.stripe.com/account/webhooks) page of the Stripe dashboard.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["WebhookEndpointDeleteParams"]
    ) -> "WebhookEndpoint":
        """
        You can also delete webhook endpoints via the [webhook endpoint management](https://dashboard.stripe.com/account/webhooks) page of the Stripe dashboard.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def list(
        cls, **params: Unpack["WebhookEndpointListParams"]
    ) -> ListObject["WebhookEndpoint"]:
        """
        Returns a list of your webhook endpoints.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["WebhookEndpointListParams"]
    ) -> ListObject["WebhookEndpoint"]:
        """
        Returns a list of your webhook endpoints.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def modify(
        cls, id: str, **params: Unpack["WebhookEndpointModifyParams"]
    ) -> "WebhookEndpoint":
        """
        Updates the webhook endpoint. You may edit the url, the list of enabled_events, and the status of your endpoint.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "WebhookEndpoint",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["WebhookEndpointModifyParams"]
    ) -> "WebhookEndpoint":
        """
        Updates the webhook endpoint. You may edit the url, the list of enabled_events, and the status of your endpoint.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "WebhookEndpoint",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["WebhookEndpointRetrieveParams"]
    ) -> "WebhookEndpoint":
        """
        Retrieves the webhook endpoint with the given ID.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["WebhookEndpointRetrieveParams"]
    ) -> "WebhookEndpoint":
        """
        Retrieves the webhook endpoint with the given ID.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance
