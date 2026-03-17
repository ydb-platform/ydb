from __future__ import annotations

import base64
import json
import logging
import re
from datetime import datetime
from functools import lru_cache
from urllib.parse import urlparse
from urllib.request import urlopen

from django.http import HttpRequest, HttpResponse, JsonResponse

from post_office.models import RecipientDeliveryStatus
from post_office.settings import get_webhook_config
from post_office.webhooks.base import BaseWebhookHandler, ESPEvent

logger = logging.getLogger(__name__)

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding
except ImportError:  # pragma: no cover - optional dependency
    x509 = None
    hashes = None
    padding = None


# Strict regex for SNS certificate URLs
# Must be sns.<region>.amazonaws.com where region follows AWS naming conventions
_SNS_DOMAIN_REGEX = re.compile(r'^sns\.[a-z]{2}(-gov)?-[a-z]+-\d+\.amazonaws\.com$')


def _is_valid_cert_url(cert_url: str) -> bool:
    try:
        parsed = urlparse(cert_url)
    except Exception:
        return False

    if parsed.scheme != 'https':
        return False

    if not _SNS_DOMAIN_REGEX.match(parsed.netloc.lower()):
        return False

    if not parsed.path.endswith('.pem'):
        return False

    return True


@lru_cache(maxsize=128)
def _get_aws_certificate(cert_url: str):
    if not _is_valid_cert_url(cert_url):
        raise ValueError(f'Invalid SNS SigningCertURL: {cert_url}')

    try:
        with urlopen(cert_url, timeout=5) as response:
            cert_pem = response.read()
    except Exception as exc:
        raise RuntimeError('Failed to fetch SNS SigningCertURL') from exc

    try:
        certificate = x509.load_pem_x509_certificate(cert_pem)
    except Exception as exc:
        raise RuntimeError('Failed to load SNS signing certificate') from exc

    return certificate.public_key()


def verify_ses_signature(payload: dict) -> bool:
    """
    Returns True if valid, False if invalid. Logs errors instead of raising.
    """
    try:
        cert_url = payload.get('SigningCertURL')
        if not cert_url:
            logger.error('SES Webhook: No SigningCertURL')
            return False

        signature_version = payload.get('SignatureVersion')
        if not signature_version:
            logger.error('SES Webhook: No SignatureVersion')
            return False

        if signature_version == '1':
            hash_alg = hashes.SHA1()
        elif signature_version == '2':
            hash_alg = hashes.SHA256()
        else:
            logger.warning('SES Webhook: Unsupported SignatureVersion %s', signature_version)
            return False

        public_key = _get_aws_certificate(cert_url)

        msg_type = payload.get('Type')
        fields = []
        if msg_type == 'Notification':
            fields = ['Message', 'MessageId', 'Subject', 'Timestamp', 'TopicArn', 'Type']
        elif msg_type in ('SubscriptionConfirmation', 'UnsubscribeConfirmation'):
            fields = ['Message', 'MessageId', 'SubscribeURL', 'Timestamp', 'Token', 'TopicArn', 'Type']

        string_to_sign = ''.join(f'{key}\n{payload[key]}\n' for key in fields if key in payload)
        if not string_to_sign:
            logger.error('SES Webhook: Unable to build string-to-sign')
            return False

        signature = base64.b64decode(payload.get('Signature', ''))
        public_key.verify(
            signature,
            string_to_sign.encode('utf-8'),
            padding.PKCS1v15(),
            hash_alg,
        )
        return True
    except Exception as exc:
        logger.warning('SES Signature Verification Failed: %s', exc)
        return False


class SESWebhookHandler(BaseWebhookHandler):
    """
    Webhook handler for AWS SES via SNS.

    SES sends notifications through SNS with the following structure:
    {
        "Type": "Notification",
        "MessageId": "...",
        "Message": "{\"notificationType\":\"Delivery\", ...}",  # JSON string
        ...
    }

    For subscription confirmation:
    {
        "Type": "SubscriptionConfirmation",
        "SubscribeURL": "https://...",
        ...
    }

    The inner Message contains:
    {
        "notificationType": "Delivery",  # or "Bounce", "Complaint"
        "mail": {
            "messageId": "...",
            "destination": ["recipient@example.com"],
            "commonHeaders": {
                "subject": "Test Subject"
            }
        },
        "delivery": {...},  # or "bounce": {...}, "complaint": {...}
    }
    """

    def verify_signature(self, request: HttpRequest) -> bool:
        """
        Verify AWS SNS message signature using X.509 certificate.
        """
        config = get_webhook_config('SES')
        if not config.get('VERIFY_SIGNATURE', True):
            return True

        if x509 is None:
            raise RuntimeError('cryptography is required for SES signature verification')

        try:
            payload = json.loads(request.body)
        except json.JSONDecodeError:
            logger.warning('Invalid JSON payload for SES signature verification')
            return False

        signature = payload.get('Signature')
        cert_url = payload.get('SigningCertURL')

        if not signature or not cert_url:
            logger.warning('SNS signature fields missing from payload')
            return False

        return verify_ses_signature(payload)

    def post(self, request: HttpRequest) -> HttpResponse:
        """Handle incoming SNS POST request, including subscription confirmation."""
        if not self.verify_signature(request):
            logger.warning('SNS signature verification failed')
            return JsonResponse({'error': 'Invalid signature'}, status=401)

        try:
            payload = json.loads(request.body)
        except json.JSONDecodeError as e:
            logger.error(f'Failed to parse SNS payload: {e}')
            return JsonResponse({'error': 'Invalid JSON'}, status=400)

        message_type = payload.get('Type', '')

        # Handle subscription confirmation
        if message_type == 'SubscriptionConfirmation':
            return self._handle_subscription_confirmation(payload)

        # Handle unsubscribe confirmation
        if message_type == 'UnsubscribeConfirmation':
            logger.info('Received SNS UnsubscribeConfirmation')
            return JsonResponse({'status': 'ok'})

        # Handle normal notifications
        if message_type == 'Notification':
            return self._handle_notification(payload)

        logger.warning(f'Unknown SNS message type: {message_type}')
        return JsonResponse({'error': 'Unknown message type'}, status=400)

    def _handle_subscription_confirmation(self, payload: dict) -> HttpResponse:
        """
        Handle SNS subscription confirmation.

        In production, you should fetch the SubscribeURL to confirm.
        For security, this should be done manually or with proper validation.
        """
        subscribe_url = payload.get('SubscribeURL')
        topic_arn = payload.get('TopicArn')
        logger.info(f'SNS subscription confirmation received. TopicArn: {topic_arn}, SubscribeURL: {subscribe_url}')
        # Return 200 to acknowledge receipt
        # Actual confirmation should be done by fetching SubscribeURL
        return JsonResponse(
            {
                'status': 'subscription_confirmation_received',
                'topic_arn': topic_arn,
                'subscribe_url': subscribe_url,
            }
        )

    def _handle_notification(self, payload: dict) -> HttpResponse:
        """Handle SNS notification containing SES event."""
        try:
            events = self.parse_events(payload)
        except Exception as e:
            logger.exception(f'Error parsing SES events: {e}')
            return JsonResponse({'error': 'Failed to parse events'}, status=400)

        self.handle_events(events, payload)

        return JsonResponse({'status': 'ok', 'processed': len(events)})

    def parse_events(self, request: HttpRequest | dict) -> list[ESPEvent]:
        """Parse SES webhook payload into ESPEvent objects."""
        if isinstance(request, HttpRequest):
            try:
                payload = json.loads(request.body)
            except json.JSONDecodeError:
                logger.error('Failed to parse SES webhook JSON')
                raise
        else:
            payload = request

        events = []

        # Extract the inner Message (it's a JSON string)
        message_str = payload.get('Message', '')
        if not message_str:
            logger.warning('No Message in SNS payload')
            return events

        try:
            message = json.loads(message_str)
        except json.JSONDecodeError:
            logger.error('Failed to parse inner SNS Message JSON')
            return events

        notification_type = message.get('notificationType', '')
        mail_data = message.get('mail', {})

        # Get common mail info
        message_id = mail_data.get('messageId')
        destinations = mail_data.get('destination', [])
        common_headers = mail_data.get('commonHeaders', {})
        subject = common_headers.get('subject')

        # Parse timestamp
        timestamp = None
        if ts := mail_data.get('timestamp'):
            try:
                timestamp = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass

        # Process based on notification type
        if notification_type == 'Delivery':
            delivery_data = message.get('delivery', {})
            recipients = delivery_data.get('recipients', destinations)
            for recipient in recipients:
                events.append(
                    ESPEvent(
                        raw_event='Delivery',
                        delivery_status=RecipientDeliveryStatus.DELIVERED,
                        recipient=recipient,
                        message_id=message_id,
                        timestamp=timestamp,
                        subject=subject,
                        to_addresses=destinations,
                    )
                )

        elif notification_type == 'Bounce':
            bounce_data = message.get('bounce', {})
            bounce_type = bounce_data.get('bounceType', 'Permanent')
            bounced_recipients = bounce_data.get('bouncedRecipients', [])

            if bounce_type == 'Transient':
                status = RecipientDeliveryStatus.SOFT_BOUNCED
            elif bounce_type == 'Undetermined':
                status = RecipientDeliveryStatus.UNDETERMINED_BOUNCED
            else:
                status = RecipientDeliveryStatus.HARD_BOUNCED

            for recipient_info in bounced_recipients:
                recipient = recipient_info.get('emailAddress', '')
                if recipient:
                    events.append(
                        ESPEvent(
                            raw_event=f'Bounce:{bounce_type}',
                            delivery_status=status,
                            recipient=recipient,
                            message_id=message_id,
                            timestamp=timestamp,
                            subject=subject,
                            to_addresses=destinations,
                        )
                    )

        elif notification_type == 'Complaint':
            complaint_data = message.get('complaint', {})
            complained_recipients = complaint_data.get('complainedRecipients', [])

            for recipient_info in complained_recipients:
                recipient = recipient_info.get('emailAddress', '')
                if recipient:
                    events.append(
                        ESPEvent(
                            raw_event='Complaint',
                            delivery_status=RecipientDeliveryStatus.SPAM_COMPLAINT,
                            recipient=recipient,
                            message_id=message_id,
                            timestamp=timestamp,
                            subject=subject,
                            to_addresses=destinations,
                        )
                    )

        else:
            logger.debug(f'Ignoring unmapped SES notification type: {notification_type}')

        return events
