from django.conf import settings


APP_MODULE_NAME = getattr(settings, 'SITEMESSAGE_APP_MODULE_NAME', 'sitemessages')
"""Module name to search sitemessage preferences in."""

INIT_BUILTIN_MESSAGE_TYPES = getattr(settings, 'SITEMESSAGE_INIT_BUILTIN_MESSAGE_TYPES', True)
"""Whether to register builtin message types."""

EMAIL_BACKEND_MESSAGES_PRIORITY = getattr(settings, 'SITEMESSAGE_EMAIL_BACKEND_MESSAGES_PRIORITY', None)
"""Priority for messages sent by Django Email backend (sitemessage.backends.EmailBackend)."""

SHORTCUT_EMAIL_MESSENGER_TYPE = getattr(settings, 'SITEMESSAGE_SHORTCUT_EMAIL_MESSENGER_TYPE', 'smtp')
"""Messenger type alias for messages sent with `schedule_email` shortcut."""

SHORTCUT_EMAIL_MESSAGE_TYPE = getattr(settings, 'SITEMESSAGE_SHORTCUT_EMAIL_MESSAGE_TYPE', None)
"""Message type alias to be used for messages sent with `schedule_email` shortcut."""

SITE_URL = getattr(settings, 'SITEMESSAGE_SITE_URL', None)
"""Site URL to use in messages."""
