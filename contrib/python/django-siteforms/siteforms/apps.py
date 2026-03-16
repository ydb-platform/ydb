from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class SiteformsConfig(AppConfig):
    """Application configuration."""

    name = 'siteforms'
    verbose_name = _('Siteforms')
