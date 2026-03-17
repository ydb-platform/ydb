__all__ = [
    'WebhookSubscription',
]
from datetime import datetime
from enum import unique

from .primitives.base import BaseTolokaObject
from ..util._codegen import attribute
from ..util._extendable_enum import ExtendableStrEnum


class WebhookSubscription(BaseTolokaObject):
    """A subscription to an event in Toloka.

    For examples, you can receive notifications when a pool is closed or a task's status changes.
    Learn more about [notifications](https://toloka.ai/docs/api/using-webhook-subscriptions/).

    Attributes:
        id: The ID of the subscription. Read-only field.
        webhook_url: The URL to which notifications are sent.
        event_type: The event type.
        pool_id: The ID of the pool that the subscription was created for.
        created: The UTC date and time when the subscription was created. Read-only field.
    """

    @unique
    class EventType(ExtendableStrEnum):
        """An event type.

        Attributes:
            POOL_CLOSED: A pool is closed.
            DYNAMIC_OVERLAP_COMPLETED: An aggregated result is ready for a task with a dynamic overlap.
            ASSIGNMENT_CREATED: A task is created.
            ASSIGNMENT_SUBMITTED: A task is completed and waiting for acceptance by a requester.
            ASSIGNMENT_SKIPPED: A task was taken by a Toloker who skipped it and didn't return to it.
            ASSIGNMENT_EXPIRED: A task was taken by a Toloker who didn't complete it within the time limit or rejected it before it expired.
            ASSIGNMENT_APPROVED: A task was completed by a Toloker and approved by a requester.
            ASSIGNMENT_REJECTED: A task was completed by a Toloker but rejected by a requester.
        """

        POOL_CLOSED = 'POOL_CLOSED'
        DYNAMIC_OVERLAP_COMPLETED = 'DYNAMIC_OVERLAP_COMPLETED'
        ASSIGNMENT_CREATED = 'ASSIGNMENT_CREATED'
        ASSIGNMENT_SUBMITTED = 'ASSIGNMENT_SUBMITTED'
        ASSIGNMENT_SKIPPED = 'ASSIGNMENT_SKIPPED'
        ASSIGNMENT_EXPIRED = 'ASSIGNMENT_EXPIRED'
        ASSIGNMENT_APPROVED = 'ASSIGNMENT_APPROVED'
        ASSIGNMENT_REJECTED = 'ASSIGNMENT_REJECTED'

    webhook_url: str
    event_type: EventType = attribute(autocast=True)
    pool_id: str
    secret_key: str

    # Readonly
    id: str = attribute(readonly=True)
    created: datetime = attribute(readonly=True)
