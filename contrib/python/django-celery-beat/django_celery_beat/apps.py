"""Django Application configuration."""
from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _

__all__ = ['BeatConfig']


class BeatConfig(AppConfig):
    """Default configuration for django_celery_beat app."""

    name = 'django_celery_beat'
    label = 'django_celery_beat'
    verbose_name = _('Periodic Tasks')
    default_auto_field = 'django.db.models.AutoField'
