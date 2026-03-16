"""
Helpers to load Django lazily when Django settings can't be configured.
"""

import os
import sys

import pytest


def skip_if_no_django():
    """Raises a skip exception when no Django settings are available"""
    if not django_settings_is_configured():
        pytest.skip("no Django settings")


def django_settings_is_configured():
    """Return whether the Django settings module has been configured.

    This uses either the DJANGO_SETTINGS_MODULE environment variable, or the
    configured flag in the Django settings object if django.conf has already
    been imported.
    """
    ret = bool(os.environ.get("DJANGO_SETTINGS_MODULE"))

    if not ret and "django.conf" in sys.modules:
        return sys.modules["django.conf"].settings.configured

    return ret


def get_django_version():
    return __import__("django").VERSION
