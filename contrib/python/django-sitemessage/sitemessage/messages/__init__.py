from ..utils import register_message_types


def register_builtin_message_types():
    """Registers the built-in message types."""
    from .plain import PlainTextMessage
    from .email import EmailTextMessage, EmailHtmlMessage
    register_message_types(PlainTextMessage, EmailTextMessage, EmailHtmlMessage)
