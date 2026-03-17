"""Static files."""
from __future__ import absolute_import, unicode_literals
import os


def get_file(*args):
    # type: (*str) -> str
    """Get filename for static file."""
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), *args)


def get_file_resource(*args):
    # type: (*str) -> str
    """Get resource key for static file."""
    return os.path.join(os.path.dirname(__file__), *args)


def logo():
    # type: () -> bytes
    """Celery logo image."""
    return get_file('celery_128.png')


def logo_resource():
    # type: () -> bytes
    """Celery logo image."""
    return get_file_resource('celery_128.png')
