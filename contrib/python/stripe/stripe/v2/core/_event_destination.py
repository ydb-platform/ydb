# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar, Dict, List, Optional
from typing_extensions import Literal


class EventDestination(StripeObject):
    """
    Set up an event destination to receive events from Stripe across multiple destination types, including [webhook endpoints](https://docs.stripe.com/webhooks) and [Amazon EventBridge](https://docs.stripe.com/event-destinations/eventbridge). Event destinations support receiving [thin events](https://docs.stripe.com/api/v2/events) and [snapshot events](https://docs.stripe.com/api/events).
    """

    OBJECT_NAME: ClassVar[Literal["v2.core.event_destination"]] = (
        "v2.core.event_destination"
    )

    class AmazonEventbridge(StripeObject):
        aws_account_id: str
        """
        The AWS account ID.
        """
        aws_event_source_arn: str
        """
        The ARN of the AWS event source.
        """
        aws_event_source_status: Literal[
            "active", "deleted", "pending", "unknown"
        ]
        """
        The state of the AWS event source.
        """

    class StatusDetails(StripeObject):
        class Disabled(StripeObject):
            reason: Literal["no_aws_event_source_exists", "user"]
            """
            Reason event destination has been disabled.
            """

        disabled: Optional[Disabled]
        """
        Details about why the event destination has been disabled.
        """
        _inner_class_types = {"disabled": Disabled}

    class WebhookEndpoint(StripeObject):
        signing_secret: Optional[str]
        """
        The signing secret of the webhook endpoint, only includable on creation.
        """
        url: Optional[str]
        """
        The URL of the webhook endpoint, includable.
        """

    amazon_eventbridge: Optional[AmazonEventbridge]
    """
    Amazon EventBridge configuration.
    """
    created: str
    """
    Time at which the object was created.
    """
    description: str
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
    events_from: Optional[List[Literal["other_accounts", "self"]]]
    """
    Where events should be routed from.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Metadata.
    """
    name: str
    """
    Event destination name.
    """
    object: Literal["v2.core.event_destination"]
    """
    String representing the object's type. Objects of the same type share the same value of the object field.
    """
    snapshot_api_version: Optional[str]
    """
    If using the snapshot event payload, the API version events are rendered as.
    """
    status: Literal["disabled", "enabled"]
    """
    Status. It can be set to either enabled or disabled.
    """
    status_details: Optional[StatusDetails]
    """
    Additional information about event destination status.
    """
    type: Literal["amazon_eventbridge", "webhook_endpoint"]
    """
    Event destination type.
    """
    updated: str
    """
    Time at which the object was last updated.
    """
    webhook_endpoint: Optional[WebhookEndpoint]
    """
    Webhook endpoint configuration.
    """
    _inner_class_types = {
        "amazon_eventbridge": AmazonEventbridge,
        "status_details": StatusDetails,
        "webhook_endpoint": WebhookEndpoint,
    }
