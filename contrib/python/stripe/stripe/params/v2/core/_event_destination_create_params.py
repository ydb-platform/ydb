# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class EventDestinationCreateParams(TypedDict):
    description: NotRequired[str]
    """
    An optional description of what the event destination is used for.
    """
    enabled_events: List[str]
    """
    The list of events to enable for this endpoint.
    """
    event_payload: Literal["snapshot", "thin"]
    """
    Payload type of events being subscribed to.
    """
    events_from: NotRequired[List[Literal["other_accounts", "self"]]]
    """
    Where events should be routed from.
    """
    include: NotRequired[
        List[
            Literal["webhook_endpoint.signing_secret", "webhook_endpoint.url"]
        ]
    ]
    """
    Additional fields to include in the response.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Metadata.
    """
    name: str
    """
    Event destination name.
    """
    snapshot_api_version: NotRequired[str]
    """
    If using the snapshot event payload, the API version events are rendered as.
    """
    type: Literal["amazon_eventbridge", "webhook_endpoint"]
    """
    Event destination type.
    """
    amazon_eventbridge: NotRequired[
        "EventDestinationCreateParamsAmazonEventbridge"
    ]
    """
    Amazon EventBridge configuration.
    """
    webhook_endpoint: NotRequired[
        "EventDestinationCreateParamsWebhookEndpoint"
    ]
    """
    Webhook endpoint configuration.
    """


class EventDestinationCreateParamsAmazonEventbridge(TypedDict):
    aws_account_id: str
    """
    The AWS account ID.
    """
    aws_region: str
    """
    The region of the AWS event source.
    """


class EventDestinationCreateParamsWebhookEndpoint(TypedDict):
    url: str
    """
    The URL of the webhook endpoint.
    """
