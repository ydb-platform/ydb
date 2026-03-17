from typing import Union

from .messages.email import EmailHtmlMessage, EmailTextMessage
from .settings import SHORTCUT_EMAIL_MESSENGER_TYPE, SHORTCUT_EMAIL_MESSAGE_TYPE
from .toolbox import schedule_messages, recipients, get_registered_message_type, TypeMessages
from .utils import TypeRecipients, TypeUser


def schedule_email(
        message: Union[TypeMessages, dict],
        to: TypeRecipients,
        subject: str = None,
        sender: TypeUser = None,
        priority: int = None
):
    """Schedules an email message for delivery.

    :param message: str or dict: use str for simple text email;
        dict - to compile email from a template (default: `sitemessage/messages/email_html__smtp.html`).
    :param to: recipients addresses or Django User model heir instances
    :param subject: email subject
    :param sender: User model heir instance
    :param priority: number describing message priority. If set overrides priority provided with message type.

    """
    if SHORTCUT_EMAIL_MESSAGE_TYPE:
        message_cls = get_registered_message_type(SHORTCUT_EMAIL_MESSAGE_TYPE)

    else:

        if isinstance(message, dict):
            message_cls = EmailHtmlMessage
        else:
            message_cls = EmailTextMessage

    schedule_messages(
        message_cls(subject, message),
        recipients(SHORTCUT_EMAIL_MESSENGER_TYPE, to),
        sender=sender, priority=priority
    )


def schedule_jabber_message(message: TypeMessages, to: TypeRecipients, sender: TypeUser = None, priority: int = None):
    """Schedules Jabber XMPP message for delivery.

    :param message: text to send.
    :param to: recipients addresses or Django User model heir instances with `email` attributes.
    :param sender: User model heir instance
    :param priority: number describing message priority. If set overrides priority provided with message type.

    """
    schedule_messages(message, recipients('xmppsleek', to), sender=sender, priority=priority)


def schedule_tweet(message: TypeMessages, to: TypeRecipients = '', sender: TypeUser = None, priority: int = None):
    """Schedules a Tweet for delivery.

    :param message: text to send.
    :param to: recipients addresses or Django User model heir instances with `telegram` attributes.
        If supplied tweets will be @-replies.
    :param sender: User model heir instance
    :param priority: number describing message priority. If set overrides priority provided with message type.

    """
    schedule_messages(message, recipients('twitter', to), sender=sender, priority=priority)


def schedule_telegram_message(
        message: TypeMessages,
        to: TypeRecipients,
        sender: TypeUser = None,
        priority: int = None
):
    """Schedules Telegram message for delivery.

    :param message: text to send.
    :param to: recipients addresses or Django User model heir instances with `telegram` attributes.
    :param sender: User model heir instance
    :param priority: number describing message priority. If set overrides priority provided with message type.

    """
    schedule_messages(message, recipients('telegram', to), sender=sender, priority=priority)


def schedule_facebook_message(message: TypeMessages, sender: TypeUser = None, priority: int = None):
    """Schedules Facebook wall message for delivery.

    :param message: text or URL to publish.
    :param sender: User model heir instance
    :param priority: number describing message priority. If set overrides priority provided with message type.

    """
    schedule_messages(message, recipients('fb', ''), sender=sender, priority=priority)


def schedule_vkontakte_message(
        message: TypeMessages,
        to: TypeRecipients,
        sender: TypeUser = None,
        priority: int = None
):
    """Schedules VKontakte message for delivery.

    :param message: text or URL to publish on wall.
    :param to: recipients addresses or Django User model heir instances with `vk` attributes.
    :param sender: User model heir instance
    :param priority: number describing message priority. If set overrides priority provided with message type.

    """
    schedule_messages(message, recipients('vk', to), sender=sender, priority=priority)
