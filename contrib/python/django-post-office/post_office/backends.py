from collections import OrderedDict
from email.mime.base import MIMEBase
from django.core.files.base import ContentFile
from django.core.mail.backends.base import BaseEmailBackend
from .settings import get_default_priority


class EmailBackend(BaseEmailBackend):
    def open(self):
        pass

    def close(self):
        pass

    def send_messages(self, email_messages):
        """
        Queue one or more EmailMessage objects and returns the number of
        email messages sent.
        """
        from .mail import create
        from .models import STATUS, Email
        from .utils import create_attachments
        from .signals import email_queued

        if not email_messages:
            return

        default_priority = get_default_priority()
        num_sent = 0
        emails = []
        for email_message in email_messages:
            subject = email_message.subject
            from_email = email_message.from_email
            headers = email_message.extra_headers
            if email_message.reply_to:
                reply_to_header = ', '.join(str(v) for v in email_message.reply_to)
                headers.setdefault('Reply-To', reply_to_header)
            message = email_message.body  # The plaintext message is called body
            html_body = ''  # The default if no html body can be found
            if hasattr(email_message, 'alternatives') and len(email_message.alternatives) > 0:
                for alternative in email_message.alternatives:
                    if alternative[1] == 'text/html':
                        html_body = alternative[0]

            attachment_files = {}
            for attachment in email_message.attachments:
                if isinstance(attachment, MIMEBase):
                    attachment_files[attachment.get_filename()] = {
                        'file': ContentFile(attachment.get_payload()),
                        'mimetype': attachment.get_content_type(),
                        'headers': OrderedDict(attachment.items()),
                    }
                else:
                    attachment_files[attachment[0]] = ContentFile(attachment[1])

            email = create(
                sender=from_email,
                recipients=email_message.to,
                cc=email_message.cc,
                bcc=email_message.bcc,
                subject=subject,
                message=message,
                html_message=html_body,
                headers=headers,
            )

            if attachment_files:
                attachments = create_attachments(attachment_files)

                email.attachments.add(*attachments)

            emails.append(email)

            if default_priority == 'now':
                status = email.dispatch()
                if status == STATUS.sent:
                    num_sent += 1

        if default_priority != 'now':
            email_queued.send(sender=Email, emails=emails)

        return num_sent
