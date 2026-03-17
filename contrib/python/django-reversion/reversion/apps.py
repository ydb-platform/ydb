from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class ReversionConfig(AppConfig):
    name = 'reversion'
    verbose_name = _('Reversion')
    default_auto_field = 'django.db.models.AutoField'
