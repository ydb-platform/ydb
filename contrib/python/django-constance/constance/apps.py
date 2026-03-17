from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class ConstanceConfig(AppConfig):
    name = 'constance'
    verbose_name = _('Constance')
    default_auto_field = 'django.db.models.AutoField'

    def ready(self):
        from . import checks

