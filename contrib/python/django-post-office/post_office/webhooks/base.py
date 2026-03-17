from __future__ import annotations

import json
import logging
from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt

from post_office.models import RecipientDeliveryStatus

logger = logging.getLogger(__name__)


# Status priority for "best status wins" logic
# Lower number = better status
STATUS_PRIORITY: dict[RecipientDeliveryStatus, int] = {
    RecipientDeliveryStatus.CLICKED: 1,
    RecipientDeliveryStatus.OPENED: 2,
    RecipientDeliveryStatus.DELIVERED: 3,
    RecipientDeliveryStatus.ACCEPTED: 4,
    RecipientDeliveryStatus.DEFERRED: 5,
    RecipientDeliveryStatus.SOFT_BOUNCED: 6,
    RecipientDeliveryStatus.UNDETERMINED_BOUNCED: 7,
    RecipientDeliveryStatus.HARD_BOUNCED: 8,
    RecipientDeliveryStatus.UNSUBSCRIBED: 9,
    RecipientDeliveryStatus.SPAM_COMPLAINT: 10,
}


@dataclass
class ESPEvent:
    """
    Normalized event data from an Email Service Provider webhook.
    """

    # Raw event name from the ESP (e.g., 'delivered', 'bounced').
    raw_event: str
    # Normalized status from RecipientDeliveryStatus enum.
    delivery_status: RecipientDeliveryStatus
    # The recipient email address this event applies to.
    recipient: str
    # Message-ID header value for matching to Email record.
    message_id: str | None = None
    timestamp: datetime | None = None
    subject: str | None = None
    to_addresses: list[str] | None = None


@method_decorator(csrf_exempt, name='dispatch')
class BaseWebhookHandler(View):
    """
    Base class for ESP webhook handlers.

    Subclasses must implement:
    - verify_signature(request) -> bool
    - parse_events(request) -> list[ESPEvent]

    Users can override:
    - handle_events(events, payload) for custom event processing
    """

    http_method_names = ['post']

    @abstractmethod
    def verify_signature(self, request: HttpRequest) -> bool:
        """
        Verify the webhook signature/authentication.

        Returns True if the request is authenticated, False otherwise.
        Subclasses should check configuration for VERIFY_SIGNATURE setting.
        """
        raise NotImplementedError

    @abstractmethod
    def parse_events(self, request: HttpRequest) -> list[ESPEvent]:
        """
        Parse the webhook payload into a list of ESPEvent objects.

        Args:
            request: The incoming HTTP request

        Returns:
            List of normalized ESPEvent objects
        """
        raise NotImplementedError

    def handle_events(self, events: list[ESPEvent], payload: dict[str, Any] | None) -> None:
        """
        User-overridable hook for custom event processing.

        Called with all parsed events from the webhook and the raw payload.
        Override this method to implement your own logic for handling events
        (e.g., updating Email records, creating logs, sending notifications,
        etc.).

        Args:
            events: List of parsed ESPEvent objects
            payload: Raw webhook payload for debugging/logging
        """
        pass

    def post(self, request: HttpRequest) -> HttpResponse:
        """Handle incoming webhook POST request."""
        # Verify signature
        if not self.verify_signature(request):
            logger.warning('Webhook signature verification failed')
            return JsonResponse({'error': 'Invalid signature'}, status=401)

        # Parse events
        try:
            events = self.parse_events(request)
        except json.JSONDecodeError as e:
            logger.error(f'Failed to parse webhook payload: {e}')
            return JsonResponse({'error': 'Invalid JSON'}, status=400)
        except Exception as e:
            logger.exception(f'Error parsing webhook events: {e}')
            return JsonResponse({'error': 'Failed to parse events'}, status=400)

        # Call user hook
        self.handle_events(events, payload=None)

        return JsonResponse({'status': 'ok', 'processed': len(events)})
