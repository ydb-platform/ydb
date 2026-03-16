from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class SitemessageConfig(AppConfig):
    """Sitemessage configuration."""

    name = 'sitemessage'
    verbose_name = _('Messaging')

    def ready(self):
        from sitemessage.utils import import_project_sitemessage_modules
        import_project_sitemessage_modules()

        from sitemessage.settings import INIT_BUILTIN_MESSAGE_TYPES
        if INIT_BUILTIN_MESSAGE_TYPES:
            from sitemessage.messages import register_builtin_message_types
            register_builtin_message_types()
