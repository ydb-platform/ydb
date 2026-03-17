"""Django Application configuration."""
from __future__ import absolute_import, unicode_literals

from django.apps import AppConfig
from django.utils.translation import ugettext_lazy as _

__all__ = ['BeatConfig']


class BeatConfig(AppConfig):
    """Default configuration for django_celery_beat app."""

    name = 'django_celery_beat'
    label = 'django_celery_beat'
    verbose_name = _('Periodic Tasks')
