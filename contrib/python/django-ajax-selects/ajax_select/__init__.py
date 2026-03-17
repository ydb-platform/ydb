"""JQuery-Ajax Autocomplete fields for Django Forms."""

__version__ = "3.0.2"
__author__ = "crucialfelix"
__contact__ = "crucialfelix@gmail.com"
__homepage__ = "https://github.com/crucialfelix/django-ajax-selects/"

from ajax_select.registry import registry, register  # noqa
from ajax_select.helpers import make_ajax_form, make_ajax_field  # noqa
from ajax_select.lookup_channel import LookupChannel  # noqa

# It will load all your lookups.py modules
# and any specified in settings.AJAX_LOOKUP_CHANNELS
# It will do this after all apps are imported.
from django.apps import AppConfig  # noqa
from django import VERSION

if VERSION < (3,):
    default_app_config = "ajax_select.apps.AjaxSelectConfig"
