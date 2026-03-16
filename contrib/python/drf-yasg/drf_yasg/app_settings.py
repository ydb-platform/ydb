from django.conf import settings
from rest_framework.settings import perform_import

SWAGGER_DEFAULTS = {
    "DEFAULT_GENERATOR_CLASS": "drf_yasg.generators.OpenAPISchemaGenerator",
    "DEFAULT_AUTO_SCHEMA_CLASS": "drf_yasg.inspectors.SwaggerAutoSchema",
    "DEFAULT_FIELD_INSPECTORS": [
        "drf_yasg.inspectors.CamelCaseJSONFilter",
        "drf_yasg.inspectors.RecursiveFieldInspector",
        "drf_yasg.inspectors.ReferencingSerializerInspector",
        "drf_yasg.inspectors.ChoiceFieldInspector",
        "drf_yasg.inspectors.FileFieldInspector",
        "drf_yasg.inspectors.DictFieldInspector",
        "drf_yasg.inspectors.JSONFieldInspector",
        "drf_yasg.inspectors.HiddenFieldInspector",
        "drf_yasg.inspectors.RelatedFieldInspector",
        "drf_yasg.inspectors.SerializerMethodFieldInspector",
        "drf_yasg.inspectors.SimpleFieldInspector",
        "drf_yasg.inspectors.StringDefaultFieldInspector",
    ],
    "DEFAULT_FILTER_INSPECTORS": [
        "drf_yasg.inspectors.DrfAPICompatInspector",
        "drf_yasg.inspectors.CoreAPICompatInspector",
    ],
    "DEFAULT_PAGINATOR_INSPECTORS": [
        "drf_yasg.inspectors.DjangoRestResponsePagination",
        "drf_yasg.inspectors.DrfAPICompatInspector",
        "drf_yasg.inspectors.CoreAPICompatInspector",
    ],
    "DEFAULT_SPEC_RENDERERS": [
        "drf_yasg.renderers.SwaggerYAMLRenderer",
        "drf_yasg.renderers.SwaggerJSONRenderer",
        "drf_yasg.renderers.OpenAPIRenderer",
    ],
    "EXCLUDED_MEDIA_TYPES": ["html"],
    "DEFAULT_INFO": None,
    "DEFAULT_API_URL": None,
    "USE_SESSION_AUTH": True,
    "USE_COMPAT_RENDERERS": getattr(settings, "SWAGGER_USE_COMPAT_RENDERERS", True),
    "CSRF_COOKIE_NAME": settings.CSRF_COOKIE_NAME,
    "CSRF_HEADER_NAME": settings.CSRF_HEADER_NAME,
    "SECURITY_DEFINITIONS": {"Basic": {"type": "basic"}},
    "SECURITY_REQUIREMENTS": None,
    "LOGIN_URL": getattr(settings, "LOGIN_URL", None),
    "LOGOUT_URL": "/accounts/logout/",
    "SPEC_URL": None,
    "VALIDATOR_URL": "",
    "PERSIST_AUTH": False,
    "REFETCH_SCHEMA_WITH_AUTH": False,
    "REFETCH_SCHEMA_ON_LOGOUT": False,
    "FETCH_SCHEMA_WITH_QUERY": True,
    "OPERATIONS_SORTER": None,
    "TAGS_SORTER": None,
    "DOC_EXPANSION": "list",
    "DEEP_LINKING": False,
    "SHOW_EXTENSIONS": True,
    "DEFAULT_MODEL_RENDERING": "model",
    "DEFAULT_MODEL_DEPTH": 3,
    "SHOW_COMMON_EXTENSIONS": True,
    "OAUTH2_REDIRECT_URL": None,
    "OAUTH2_CONFIG": {},
    "SUPPORTED_SUBMIT_METHODS": [
        "get",
        "put",
        "post",
        "delete",
        "options",
        "head",
        "patch",
        "trace",
    ],
    "DISPLAY_OPERATION_ID": True,
}

REDOC_DEFAULTS = {
    "SPEC_URL": None,
    "LAZY_RENDERING": False,
    "HIDE_HOSTNAME": False,
    "EXPAND_RESPONSES": "all",
    "PATH_IN_MIDDLE": False,
    "NATIVE_SCROLLBARS": False,
    "REQUIRED_PROPS_FIRST": False,
    "FETCH_SCHEMA_WITH_QUERY": True,
    "HIDE_DOWNLOAD_BUTTON": False,
}

IMPORT_STRINGS = [
    "DEFAULT_GENERATOR_CLASS",
    "DEFAULT_AUTO_SCHEMA_CLASS",
    "DEFAULT_FIELD_INSPECTORS",
    "DEFAULT_FILTER_INSPECTORS",
    "DEFAULT_PAGINATOR_INSPECTORS",
    "DEFAULT_SPEC_RENDERERS",
    "DEFAULT_INFO",
]


class AppSettings:
    """
    Stolen from Django Rest Framework, removed caching for easier testing
    """

    def __init__(self, user_settings, defaults, import_strings=None):
        self._user_settings = user_settings
        self.defaults = defaults
        self.import_strings = import_strings or []

    @property
    def user_settings(self):
        return getattr(settings, self._user_settings, {})

    def __getattr__(self, attr):
        if attr not in self.defaults:
            raise AttributeError("Invalid setting: '%s'" % attr)  # pragma: no cover

        try:
            # Check if present in user settings
            val = self.user_settings[attr]
        except KeyError:
            # Fall back to defaults
            val = self.defaults[attr]

        # Coerce import strings into classes
        if attr in self.import_strings:
            val = perform_import(val, attr)

        return val


#:
swagger_settings = AppSettings(
    user_settings="SWAGGER_SETTINGS",
    defaults=SWAGGER_DEFAULTS,
    import_strings=IMPORT_STRINGS,
)

#:
redoc_settings = AppSettings(
    user_settings="REDOC_SETTINGS",
    defaults=REDOC_DEFAULTS,
    import_strings=IMPORT_STRINGS,
)
