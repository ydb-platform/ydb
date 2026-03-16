"""Application configuration."""

from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _

__all__ = ['CeleryResultConfig']


class CeleryResultConfig(AppConfig):
    """Default configuration for the django_celery_results app."""

    name = 'django_celery_results'
    label = 'django_celery_results'
    verbose_name = _('Celery Results')
    default_auto_field = 'django.db.models.AutoField'
