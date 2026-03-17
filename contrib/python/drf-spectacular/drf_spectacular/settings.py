from contextlib import contextmanager
from typing import Any, Dict

from django.conf import settings
from rest_framework.settings import APISettings, perform_import

SPECTACULAR_DEFAULTS: Dict[str, Any] = {
    # A regex specifying the common denominator for all operation paths. If
    # SCHEMA_PATH_PREFIX is set to None, drf-spectacular will attempt to estimate
    # a common prefix. Use '' to disable.
    # Mainly used for tag extraction, where paths like '/api/v1/albums' with
    # a SCHEMA_PATH_PREFIX regex '/api/v[0-9]' would yield the tag 'albums'.
    'SCHEMA_PATH_PREFIX': None,
    # Remove matching SCHEMA_PATH_PREFIX from operation path. Usually used in
    # conjunction with appended prefixes in SERVERS.
    'SCHEMA_PATH_PREFIX_TRIM': False,
    # Insert a manual path prefix to the operation path, e.g. '/service/backend'.
    # Use this for example to align paths when the API is mounted as a sub-resource
    # behind a proxy and Django is not aware of that. Alternatively, prefixes can
    # also specified via SERVERS, but this makes the operation path more explicit.
    'SCHEMA_PATH_PREFIX_INSERT': '',
    # Coercion of {pk} to {id} is controlled by SCHEMA_COERCE_PATH_PK. Additionally,
    # some libraries (e.g. drf-nested-routers) use "_pk" suffixed path variables.
    # This setting globally coerces path variables like "{user_pk}" to "{user_id}".
    'SCHEMA_COERCE_PATH_PK_SUFFIX': False,

    # Schema generation parameters to influence how components are constructed.
    # Some schema features might not translate well to your target.
    # Demultiplexing/modifying components might help alleviate those issues.
    'DEFAULT_GENERATOR_CLASS': 'drf_spectacular.generators.SchemaGenerator',
    # Create separate components for PATCH endpoints (without required list)
    'COMPONENT_SPLIT_PATCH': True,
    # Split components into request and response parts where appropriate
    # This setting is highly recommended to achieve the most accurate API
    # description, however it comes at the cost of having more components.
    'COMPONENT_SPLIT_REQUEST': False,
    # Aid client generator targets that have trouble with read-only properties.
    'COMPONENT_NO_READ_ONLY_REQUIRED': False,

    # Adds "minLength: 1" to fields that do not allow blank strings. Deactivated
    # by default because serializers do not strictly enforce this on responses and
    # so "minLength: 1" may not always accurately describe API behavior.
    # Gets implicitly enabled by COMPONENT_SPLIT_REQUEST, because this can be
    # accurately modeled when request and response components are separated.
    'ENFORCE_NON_BLANK_FIELDS': False,

    # This version string will end up the in schema header. The default OpenAPI
    # version is 3.0.3, which is heavily tested. We now also support 3.1.0,
    # which contains the same features and a few mandatory, but minor changes.
    'OAS_VERSION': '3.0.3',

    # Configuration for serving a schema subset with SpectacularAPIView
    'SERVE_URLCONF': None,
    # complete public schema or a subset based on the requesting user
    'SERVE_PUBLIC': True,
    # include schema endpoint into schema
    'SERVE_INCLUDE_SCHEMA': True,
    # list of authentication/permission classes for spectacular's views.
    'SERVE_PERMISSIONS': ['rest_framework.permissions.AllowAny'],
    # None will default to DRF's AUTHENTICATION_CLASSES
    'SERVE_AUTHENTICATION': None,

    # Dictionary of general configuration to pass to the SwaggerUI({ ... })
    # https://swagger.io/docs/open-source-tools/swagger-ui/usage/configuration/
    # The settings are serialized with json.dumps(). If you need customized JS, use a
    # string instead. The string must then contain valid JS and is passed unchanged.
    'SWAGGER_UI_SETTINGS': {
        'deepLinking': True,
    },
    # Initialize SwaggerUI with additional OAuth2 configuration.
    # https://swagger.io/docs/open-source-tools/swagger-ui/usage/oauth2/
    'SWAGGER_UI_OAUTH2_CONFIG': {},

    # Dictionary of general configuration to pass to the Redoc.init({ ... })
    # https://redocly.com/docs/redoc/config/#functional-settings
    # The settings are serialized with json.dumps(). If you need customized JS, use a
    # string instead. The string must then contain valid JS and is passed unchanged.
    'REDOC_UI_SETTINGS': {},

    # CDNs for swagger and redoc. You can change the version or even host your
    # own depending on your requirements. For self-hosting, have a look at
    # the sidecar option in the README.
    'SWAGGER_UI_DIST': 'https://cdn.jsdelivr.net/npm/swagger-ui-dist@latest',
    'SWAGGER_UI_FAVICON_HREF': 'https://cdn.jsdelivr.net/npm/swagger-ui-dist@latest/favicon-32x32.png',
    'REDOC_DIST': 'https://cdn.jsdelivr.net/npm/redoc@latest',

    # Append OpenAPI objects to path and components in addition to the generated objects
    'APPEND_PATHS': {},
    'APPEND_COMPONENTS': {},

    # STRONGLY DISCOURAGED (with the exception for the djangorestframework-api-key library)
    # please don't use this anymore as it has tricky implications that
    # are hard to get right. For authentication, OpenApiAuthenticationExtension are
    # strongly preferred because they are more robust and easy to write.
    # However if used, the list of methods is appended to every endpoint in the schema!
    'SECURITY': [],

    # Postprocessing functions that run at the end of schema generation.
    # must satisfy interface result = hook(generator, request, public, result)
    'POSTPROCESSING_HOOKS': [
        'drf_spectacular.hooks.postprocess_schema_enums'
    ],

    # Preprocessing functions that run before schema generation.
    # must satisfy interface result = hook(endpoints=result) where result
    # is a list of Tuples (path, path_regex, method, callback).
    # Example: 'drf_spectacular.hooks.preprocess_exclude_path_format'
    'PREPROCESSING_HOOKS': [],

    # Determines how operations should be sorted. If you intend to do sorting with a
    # PREPROCESSING_HOOKS, be sure to disable this setting. If configured, the sorting
    # is applied after the PREPROCESSING_HOOKS. Accepts either
    # True (drf-spectacular's alpha-sorter), False, or a callable for sort's key arg.
    'SORT_OPERATIONS': True,

    # enum name overrides. dict with keys "YourEnum" and their choice values "field.choices"
    # e.g. {'SomeEnum': ['A', 'B'], 'OtherEnum': 'import.path.to.choices'}
    'ENUM_NAME_OVERRIDES': {},
    # Adds "blank" and "null" enum choices where appropriate. disable on client generation issues
    'ENUM_ADD_EXPLICIT_BLANK_NULL_CHOICE': True,
    # Add/Append a list of (``choice value`` - choice name) to the enum description string.
    'ENUM_GENERATE_CHOICE_DESCRIPTION': True,
    # Optional suffix for generated enum.
    # e.g. {'ENUM_SUFFIX': "Type"} would produce an enum name 'StatusType'.
    'ENUM_SUFFIX': 'Enum',

    # function that returns a list of all classes that should be excluded from doc string extraction
    'GET_LIB_DOC_EXCLUDES': 'drf_spectacular.plumbing.get_lib_doc_excludes',

    # Function that returns a mocked request for view processing. For CLI usage
    # original_request will be None.
    # interface: request = build_mock_request(method, path, view, original_request, **kwargs)
    'GET_MOCK_REQUEST': 'drf_spectacular.plumbing.build_mock_request',

    # Camelize names like "operationId" and path parameter names
    # Camelization of the operation schema itself requires the addition of
    # 'drf_spectacular.contrib.djangorestframework_camel_case.camelize_serializer_fields'
    # to POSTPROCESSING_HOOKS. Please note that the hook depends on
    # ``djangorestframework_camel_case``, while CAMELIZE_NAMES itself does not.
    'CAMELIZE_NAMES': False,

    # Changes the location of the action/method on the generated OperationId. For example,
    # "POST": "group_person_list", "group_person_create"
    # "PRE":  "list_group_person", "create_group_person"
    'OPERATION_ID_METHOD_POSITION': 'POST',

    # Determines if and how free-form 'additionalProperties' should be emitted in the schema. Some
    # code generator targets are sensitive to this. None disables generic 'additionalProperties'.
    # allowed values are 'dict', 'bool', None
    'GENERIC_ADDITIONAL_PROPERTIES': 'dict',

    # Path converter schema overrides (e.g. <int:foo>). Can be used to either modify default
    # behavior or provide a schema for custom converters registered with register_converter(...).
    # Takes converter labels as keys and either basic python types, OpenApiType, or raw schemas
    # as values. Example: {'aint': OpenApiTypes.INT, 'bint': str, 'cint': {'type': ...}}
    'PATH_CONVERTER_OVERRIDES': {},

    # Determines whether operation parameters should be sorted alphanumerically or just in
    # the order they arrived. Accepts either True, False, or a callable for sort's key arg.
    'SORT_OPERATION_PARAMETERS': True,

    # @extend_schema allows to specify status codes besides 200. This functionality is usually used
    # to describe error responses, which rarely make use of list mechanics. Therefore, we suppress
    # listing (pagination and filtering) on non-2XX status codes by default. Toggle this to enable
    # list responses with ListSerializers/many=True irrespective of the status code.
    'ENABLE_LIST_MECHANICS_ON_NON_2XX': False,

    # This setting allows you to deviate from the default manager by accessing a different model
    # property. We use "objects" by default for compatibility reasons. Using "_default_manager"
    # will likely fix most issues, though you are free to choose any name.
    "DEFAULT_QUERY_MANAGER": 'objects',

    # Controls which authentication methods are exposed in the schema. If not None, will hide
    # authentication classes that are not contained in the whitelist. Use full import paths
    # like ['rest_framework.authentication.TokenAuthentication', ...].
    # Empty list ([]) will hide all authentication methods. The default None will show all.
    'AUTHENTICATION_WHITELIST': None,
    # Controls which parsers are exposed in the schema. Works analog to AUTHENTICATION_WHITELIST.
    # List of allowed parsers or None to allow all.
    'PARSER_WHITELIST': None,
    # Controls which renderers are exposed in the schema. Works analog to AUTHENTICATION_WHITELIST.
    # rest_framework.renderers.BrowsableAPIRenderer is ignored by default if whitelist is None
    'RENDERER_WHITELIST': None,

    # Option for turning off error and warn messages
    'DISABLE_ERRORS_AND_WARNINGS': False,

    # Runs exemplary schema generation and emits warnings as part of "./manage.py check --deploy"
    'ENABLE_DJANGO_DEPLOY_CHECK': True,

    # General schema metadata. Refer to spec for valid inputs
    # https://spec.openapis.org/oas/v3.0.3#openapi-object
    'TITLE': '',
    'DESCRIPTION': '',
    'TOS': None,
    # Optional: MAY contain "name", "url", "email"
    'CONTACT': {},
    # Optional: MUST contain "name", MAY contain URL
    'LICENSE': {},
    # Statically set schema version. May also be an empty string. When used together with
    # view versioning, will become '0.0.0 (v2)' for 'v2' versioned requests.
    # Set VERSION to None if only the request version should be rendered.
    'VERSION': '0.0.0',
    # Optional list of servers.
    # Each entry MUST contain "url", MAY contain "description", "variables"
    # e.g. [{'url': 'https://example.com/v1', 'description': 'Text'}, ...]
    'SERVERS': [],
    # Tags defined in the global scope
    'TAGS': [],
    # Optional: List of OpenAPI 3.1 webhooks. Each entry should be an import path to an
    # OpenApiWebhook instance.
    'WEBHOOKS': [],
    # Optional: MUST contain 'url', may contain "description"
    'EXTERNAL_DOCS': {},

    # Arbitrary specification extensions attached to the schema's info object.
    # https://swagger.io/specification/#specification-extensions
    'EXTENSIONS_INFO': {},

    # Arbitrary specification extensions attached to the schema's root object.
    # https://swagger.io/specification/#specification-extensions
    'EXTENSIONS_ROOT': {},

    # Oauth2 related settings. used for example by django-oauth2-toolkit.
    # https://spec.openapis.org/oas/v3.0.3#oauth-flows-object
    'OAUTH2_FLOWS': [],
    'OAUTH2_AUTHORIZATION_URL': None,
    'OAUTH2_TOKEN_URL': None,
    'OAUTH2_REFRESH_URL': None,
    'OAUTH2_SCOPES': None,
}

IMPORT_STRINGS = [
    'DEFAULT_GENERATOR_CLASS',
    'SERVE_AUTHENTICATION',
    'SERVE_PERMISSIONS',
    'POSTPROCESSING_HOOKS',
    'PREPROCESSING_HOOKS',
    'GET_LIB_DOC_EXCLUDES',
    'GET_MOCK_REQUEST',
    'SORT_OPERATIONS',
    'SORT_OPERATION_PARAMETERS',
    'AUTHENTICATION_WHITELIST',
    'RENDERER_WHITELIST',
    'PARSER_WHITELIST',
    'WEBHOOKS',
]


class SpectacularSettings(APISettings):
    _original_settings: Dict[str, Any] = {}

    def apply_patches(self, patches):
        for attr, val in patches.items():
            if attr.startswith('SERVE_') or attr == 'DEFAULT_GENERATOR_CLASS':
                raise AttributeError(
                    f'{attr} not allowed in custom_settings. use dedicated parameter instead.'
                )
            if attr in self.import_strings:
                val = perform_import(val, attr)
            # load and store original value, then override __dict__ entry
            self._original_settings[attr] = getattr(self, attr)
            setattr(self, attr, val)

    def clear_patches(self):
        for attr, orig_val in self._original_settings.items():
            setattr(self, attr, orig_val)
        self._original_settings = {}


spectacular_settings = SpectacularSettings(
    user_settings=getattr(settings, 'SPECTACULAR_SETTINGS', {}),  # type: ignore
    defaults=SPECTACULAR_DEFAULTS,  # type: ignore
    import_strings=IMPORT_STRINGS,
)


@contextmanager
def patched_settings(patches):
    """ temporarily patch the global spectacular settings (or do nothing) """
    if not patches:
        yield
    else:
        try:
            spectacular_settings.apply_patches(patches)
            yield
        finally:
            spectacular_settings.clear_patches()
