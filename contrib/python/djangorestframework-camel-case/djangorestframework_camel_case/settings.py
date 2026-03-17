from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from rest_framework.settings import APISettings

USER_SETTINGS = getattr(settings, "JSON_CAMEL_CASE", {})

DEFAULTS = {
    "RENDERER_CLASS": "rest_framework.renderers.JSONRenderer",
    "PARSER_CLASS": "rest_framework.parsers.JSONParser",
    "JSON_UNDERSCOREIZE": {"no_underscore_before_number": False, "ignore_fields": None, "ignore_keys": None},
}

# List of settings that may be in string import notation.
IMPORT_STRINGS = ("RENDERER_CLASS", "PARSER_CLASS")

VALID_SETTINGS = {
    "RENDERER_CLASS": (
        "rest_framework.renderers.JSONRenderer",
	"drf_orjson_renderer.renderers.ORJSONRenderer",
        "rest_framework.renderers.UnicodeJSONRenderer",
    ),
    "PARSER_CLASS": ("rest_framework.parsers.JSONParser",),
}


def validate_settings(input_settings, valid_settings):
    for setting_name, valid_values in valid_settings.items():
        input_setting = input_settings.get(setting_name)
        if input_setting and input_setting not in valid_values:
            raise ImproperlyConfigured(setting_name)


validate_settings(USER_SETTINGS, VALID_SETTINGS)

api_settings = APISettings(USER_SETTINGS, DEFAULTS, IMPORT_STRINGS)
