# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List, Optional
from typing_extensions import Literal, NotRequired, TypedDict


class EventDestinationUpdateParams(TypedDict):
    description: NotRequired[str]
    """
    An optional description of what the event destination is used for.
    """
    enabled_events: NotRequired[List[str]]
    """
    The list of events to enable for this endpoint.
    """
    include: NotRequired[List[Literal["webhook_endpoint.url"]]]
    """
    Additional fields to include in the response. Currently supports `webhook_endpoint.url`.
    """
    metadata: NotRequired[Dict[str, Optional[str]]]
    """
    Metadata.
    """
    name: NotRequired[str]
    """
    Event destination name.
    """
    webhook_endpoint: NotRequired[
        "EventDestinationUpdateParamsWebhookEndpoint"
    ]
    """
    Webhook endpoint configuration.
    """


class EventDestinationUpdateParamsWebhookEndpoint(TypedDict):
    url: str
    """
    The URL of the webhook endpoint.
    """
