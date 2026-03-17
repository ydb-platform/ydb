from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class SitetreeConfig(AppConfig):
    """Sitetree configuration."""

    name: str = 'sitetree'
    verbose_name: str = _('Site Trees')
    default_auto_field = 'django.db.models.AutoField'
