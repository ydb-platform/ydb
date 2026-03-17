from __future__ import annotations

import base64
import binascii
import json
import logging
from datetime import datetime, timezone

from django.http import HttpRequest

from post_office.models import RecipientDeliveryStatus
from post_office.settings import get_webhook_config
from post_office.webhooks.base import BaseWebhookHandler, ESPEvent

logger = logging.getLogger(__name__)

# SparkPost bounce categories mapped to delivery status
# See: https://support.sparkpost.com/docs/deliverability/bounce-classification-codes
SOFT_BOUNCE_CATEGORIES = {'Soft', 'Block'}
UNDETERMINED_BOUNCE_CATEGORIES = {'Undetermined'}
HARD_BOUNCE_CATEGORIES = {'Hard', 'Admin'}


class SparkPostWebhookHandler(BaseWebhookHandler):
    """
    Webhook handler for SparkPost.

    SparkPost sends webhooks as JSON arrays containing batched events:
    [
        {
            "msys": {
                "message_event": {
                    "type": "delivery",
                    "rcpt_to": "recipient@example.com",
                    "message_id": "...",
                    "timestamp": "1234567890",
                    ...
                }
            }
        },
        ...
    ]

    Events are wrapped in msys.message_event, msys.track_event,
    msys.unsubscribe_event, or msys.relay_event.

    Authentication is via HTTP Basic Auth with configured USERNAME/PASSWORD.
    """

    def verify_signature(self, request: HttpRequest) -> bool:
        """
        Verify SparkPost webhook using HTTP Basic Authentication.
        """
        config = get_webhook_config('SPARKPOST')
        if not config.get('VERIFY_SIGNATURE', True):
            return True

        username = config.get('USERNAME')
        password = config.get('PASSWORD')

        if not username or not password:
            logger.warning('SparkPost webhook credentials not configured')
            return False

        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Basic '):
            logger.warning('SparkPost webhook missing Basic Auth header')
            return False

        try:
            encoded_credentials = auth_header[6:]  # Strip 'Basic '
            decoded = base64.b64decode(encoded_credentials).decode('utf-8')
            provided_username, provided_password = decoded.split(':', 1)
        except (ValueError, UnicodeDecodeError, binascii.Error):
            logger.warning('SparkPost webhook invalid Basic Auth format')
            return False

        if provided_username != username or provided_password != password:
            logger.warning('SparkPost webhook authentication failed')
            return False

        return True

    def parse_events(self, request: HttpRequest) -> list[ESPEvent]:
        """Parse SparkPost webhook payload into ESPEvent objects."""
        try:
            payload = json.loads(request.body)
        except json.JSONDecodeError:
            logger.error('Failed to parse SparkPost webhook JSON')
            raise

        events = []

        # SparkPost sends an array of event wrappers
        # Handle both raw array and {"results": [...]} format
        if isinstance(payload, dict) and 'results' in payload:
            payload = payload['results']

        if not isinstance(payload, list):
            logger.warning('SparkPost payload is not a list')
            return events

        for event_wrapper in payload:
            msys = event_wrapper.get('msys', {})

            # Find the event data - could be in different keys
            event_data = None
            for key in ('message_event', 'track_event', 'unsubscribe_event', 'relay_event'):
                if key in msys:
                    event_data = msys[key]
                    break

            if not event_data:
                continue

            event = self._parse_single_event(event_data)
            if event:
                events.append(event)

        return events

    def _parse_single_event(self, event_data: dict) -> ESPEvent | None:
        """Parse a single SparkPost event into an ESPEvent."""
        event_type = event_data.get('type', '')
        recipient = event_data.get('rcpt_to', '')

        if not recipient:
            return None

        # Map SparkPost event type to RecipientDeliveryStatus
        status = self._get_delivery_status(event_type, event_data)
        if status is None:
            logger.debug(f'Ignoring unmapped SparkPost event type: {event_type}')
            return None

        # Parse timestamp (Unix epoch as string, may be fractional)
        timestamp = None
        if ts := event_data.get('timestamp'):
            try:
                timestamp = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            except (ValueError, TypeError, OSError):
                pass

        # Get message ID
        message_id = event_data.get('message_id')

        # Get subject if available
        subject = event_data.get('subject')

        return ESPEvent(
            raw_event=event_type,
            delivery_status=status,
            recipient=recipient,
            message_id=message_id,
            timestamp=timestamp,
            subject=subject,
            to_addresses=[recipient],
        )

    def _get_delivery_status(self, event_type: str, event_data: dict) -> RecipientDeliveryStatus | None:
        """Map SparkPost event type to RecipientDeliveryStatus."""
        if event_type == 'injection':
            return RecipientDeliveryStatus.ACCEPTED

        if event_type == 'delivery':
            return RecipientDeliveryStatus.DELIVERED

        if event_type in ('open', 'initial_open'):
            return RecipientDeliveryStatus.OPENED

        if event_type == 'click':
            return RecipientDeliveryStatus.CLICKED

        if event_type == 'delay':
            return RecipientDeliveryStatus.DEFERRED

        if event_type in ('bounce', 'out_of_band', 'policy_rejection'):
            return self._get_bounce_status(event_data)

        if event_type == 'spam_complaint':
            return RecipientDeliveryStatus.SPAM_COMPLAINT

        if event_type in ('list_unsubscribe', 'link_unsubscribe'):
            return RecipientDeliveryStatus.UNSUBSCRIBED

        return None

    def _get_bounce_status(self, event_data: dict) -> RecipientDeliveryStatus:
        """
        Determine bounce status based on bounce_class category.

        SparkPost bounce classification:
        - Soft, Block: Temporary issues (soft bounce)
        - Undetermined: Could not be identified
        - Hard, Admin: Permanent failures (hard bounce)
        """
        bounce_class = event_data.get('bounce_class')

        # SparkPost provides bounce_class as an integer code
        # We need to map it to a category, but the category name
        # may also be available in some webhook payloads
        # For robustness, check both raw_reason and use bounce_class ranges

        # Bounce class ranges (from SparkPost docs):
        # 1: Undetermined
        # 10, 30, 90: Hard
        # 20-29: Soft
        # 40-49: Soft (generic)
        # 50-59: Block (mail block)
        # 60-69: Soft
        # 70-79: Soft
        # 25, 80-89: Admin (policy)
        # 100: Soft

        if bounce_class is not None:
            try:
                bc = int(bounce_class)
                if bc == 1:
                    return RecipientDeliveryStatus.UNDETERMINED_BOUNCED
                if bc in (10, 30, 90):
                    return RecipientDeliveryStatus.HARD_BOUNCED
                if bc in (25,) or 80 <= bc <= 89:
                    return RecipientDeliveryStatus.HARD_BOUNCED  # Admin = permanent
                # All others are soft/block (temporary)
                return RecipientDeliveryStatus.SOFT_BOUNCED
            except (ValueError, TypeError):
                pass

        # Default to hard bounce if we can't determine
        return RecipientDeliveryStatus.HARD_BOUNCED
