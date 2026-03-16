"""
This import enables the import path of the django-jinja template backend
to have a sane default NAME in your TEMPLATES setting.
See: https://github.com/niwinz/django-jinja/pull/303
"""

from .backend import Jinja2

__all__ = ["Jinja2"]
