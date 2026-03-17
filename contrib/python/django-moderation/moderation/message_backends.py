from django.conf import settings
from django.core.mail import send_mail, send_mass_mail


class BaseMessageBackend(object):

    def send(self, **kwargs):
        raise NotImplementedError


class BaseMultipleMessageBackend(BaseMessageBackend):
    """Used to send mail to multiple users"""


class SyncMessageBackend(BaseMessageBackend):
    """Synchronous backend"""


class AsyncMessageBackend(BaseMessageBackend):
    """Asynchronous backend"""


class EmailMessageBackend(SyncMessageBackend):
    """
    Send the message through an email on the main thread
    """

    def send(self, **kwargs):
        subject = kwargs.get('subject', None)
        message = kwargs.get('message', None)
        recipient_list = kwargs.get('recipient_list', None)

        send_mail(subject=subject,
                  message=message,
                  from_email=settings.DEFAULT_FROM_EMAIL,
                  recipient_list=recipient_list,
                  fail_silently=True)


class EmailMultipleMessageBackend(SyncMessageBackend):
    """
    Send messages through emails on the main thread
    """

    def send(self, datatuples, **kwargs):
        send_mass_mail(
            tuple(tuple(
                d.get('subject', None),
                d.get('message', None),
                settings.DEFAULT_FROM_EMAIL,
                d.get('recipient_list', None))
                for d in datatuples),
            fail_silently=True)
