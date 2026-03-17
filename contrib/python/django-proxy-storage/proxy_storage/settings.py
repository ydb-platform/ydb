# -*- coding: utf-8 -*-
try:
    from django.utils import importlib
except ImportError:
    import importlib

from django.conf import settings

from proxy_storage.compat import six


USER_SETTINGS = getattr(settings, 'PROXY_STORAGE', None)

DEFAULTS = {
    'PROXY_STORAGE_CLASSES': {},
}

IMPORT_STRINGS = (
    'PROXY_STORAGE_CLASSES',
)


def perform_import(val, setting_name):
    """
    If the given setting is a string import notation,
    then perform the necessary import or imports.
    """
    if isinstance(val, six.string_types):
        return import_from_string(val, setting_name)
    elif isinstance(val, (list, tuple)):
        return [import_from_string(item, setting_name) for item in val]
    elif isinstance(val, dict):
        result = {}
        for key, value in val.items():
            result[key] = import_from_string(value, setting_name)
        return result
    return val


def import_from_string(val, setting_name):
    """
    Attempt to import a class from a string representation.
    """
    try:
        # Nod to tastypie's use of importlib.
        parts = val.split('.')
        module_path, class_name = '.'.join(parts[:-1]), parts[-1]
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    except ImportError as e:
        msg = "Could not import '%s' for API setting '%s'. %s: %s." % (val, setting_name, e.__class__.__name__, e)
        raise ImportError(msg)


class Settings(object):
    """
    A settings object, that allows settings to be accessed as properties.
    For example:

        from django_proxy_storage.settings import django_proxy_storage_settings
        print django_proxy_storage_settings.PROXY_STORAGE_CLASSES

    Any setting with string import paths will be automatically resolved
    and return the class, rather than the string literal.
    """
    def __init__(self, user_settings=None, defaults=None, import_strings=None):
        self.user_settings = user_settings or {}
        self.defaults = defaults or {}
        self.import_strings = import_strings or ()

    def __getattr__(self, attr):
        if attr not in self.defaults.keys():
            raise AttributeError("Invalid setting: '%s'" % attr)

        try:
            # Check if present in user settings
            val = self.user_settings[attr]
        except KeyError:
            # Fall back to defaults
            val = self.defaults[attr]

        # Coerce import strings into classes
        if val and attr in self.import_strings:
            val = perform_import(val, attr)

        self.validate_setting(attr, val)

        # Cache the result
        setattr(self, attr, val)
        return val

    def validate_setting(self, attr, val):
        if attr == 'FILTER_BACKEND' and val is not None:
            # Make sure we can initialize the class
            val()


proxy_storage_settings = Settings(USER_SETTINGS, DEFAULTS, IMPORT_STRINGS)
proxy_storage_settings.PROXY_STORAGE_CLASSES_INVERTED = dict(
    (value, key) for key, value in proxy_storage_settings.PROXY_STORAGE_CLASSES.items()
)
