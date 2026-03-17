from typing import List

from django.core.mail import EmailMessage
from django.core.mail.backends.base import BaseEmailBackend

from .settings import EMAIL_BACKEND_MESSAGES_PRIORITY
from .shortcuts import schedule_email


class EmailBackend(BaseEmailBackend):
    """Email backend for Django built-in mailing functions scheduling messages."""

    def send_messages(self, email_messages: List[EmailMessage]):

        if not email_messages:
            return

        sent = 0
        for message in email_messages:

            if not message.recipients():
                continue

            schedule_email(
                {'contents': message.body},
                message.recipients(),
                subject=message.subject,
                priority=EMAIL_BACKEND_MESSAGES_PRIORITY,
            )

            sent += 1

        return sent
