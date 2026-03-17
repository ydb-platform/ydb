from django.core.exceptions import ImproperlyConfigured
from django.shortcuts import resolve_url
from django.template.loader import render_to_string
from django.utils.encoding import force_str
from django.utils.functional import Promise
from rest_framework.renderers import BaseRenderer, JSONRenderer, TemplateHTMLRenderer
from rest_framework.utils import encoders, json

from .app_settings import redoc_settings, swagger_settings
from .codecs import VALIDATORS, OpenAPICodecJson, OpenAPICodecYaml
from .openapi import Swagger
from .utils import filter_none


class _SpecRenderer(BaseRenderer):
    """Base class for text renderers. Handles encoding and validation."""

    charset = "utf-8"
    validators = []
    codec_class = None

    @classmethod
    def with_validators(cls, validators):
        assert all(vld in VALIDATORS for vld in validators), (
            "allowed validators are " + ", ".join(VALIDATORS)
        )
        return type(cls.__name__, (cls,), {"validators": validators})

    def render(self, data, media_type=None, renderer_context=None):
        assert self.codec_class, "must override codec_class"
        codec = self.codec_class(self.validators)

        if not isinstance(data, Swagger):  # pragma: no cover
            # if `swagger` is not a ``Swagger`` object, it means we somehow got a
            # non-success ``Response`` in that case, it's probably better to let the
            # default ``JSONRenderer`` render it
            # see https://github.com/axnsan12/drf-yasg/issues/58
            return JSONRenderer().render(data, media_type, renderer_context)

        return codec.encode(data)


class OpenAPIRenderer(_SpecRenderer):
    """Renders the schema as a JSON document with the ``application/openapi+json``
    specific mime type."""

    media_type = "application/openapi+json"
    format = "openapi"
    codec_class = OpenAPICodecJson


class SwaggerJSONRenderer(_SpecRenderer):
    """Renders the schema as a JSON document with the generic ``application/json`` mime
    type."""

    media_type = "application/json"
    format = "json"
    codec_class = OpenAPICodecJson


class SwaggerYAMLRenderer(_SpecRenderer):
    """Renders the schema as a YAML document."""

    media_type = "application/yaml"
    format = "yaml"
    codec_class = OpenAPICodecYaml


class _UIRenderer(BaseRenderer):
    """Base class for web UI renderers. Handles loading and passing settings to the
    appropriate template."""

    media_type = "text/html"
    charset = "utf-8"
    template = ""

    def render(self, swagger, accepted_media_type=None, renderer_context=None):
        if not isinstance(swagger, Swagger):  # pragma: no cover
            try:
                # if `swagger` is not a ``Swagger`` object, it means we somehow got a
                # non-success ``Response`` in that case, it's probably better to let the
                # default ``TemplateHTMLRenderer`` render it
                # see https://github.com/axnsan12/drf-yasg/issues/58
                return TemplateHTMLRenderer().render(
                    swagger, accepted_media_type, renderer_context
                )
            except ImproperlyConfigured:
                # Fall back to using eg '404 Not Found'
                response = renderer_context["response"]
                return "%d %s" % (response.status_code, response.status_text.title())

        self.set_context(renderer_context, swagger)
        return render_to_string(
            self.template, renderer_context, renderer_context["request"]
        )

    def set_context(self, renderer_context, swagger=None):
        renderer_context["title"] = swagger.info.title or "" if swagger else ""
        renderer_context["version"] = swagger.info.version or "" if swagger else ""
        renderer_context["oauth2_config"] = json.dumps(
            self.get_oauth2_config(), cls=encoders.JSONEncoder
        )
        renderer_context["USE_SESSION_AUTH"] = swagger_settings.USE_SESSION_AUTH
        renderer_context.update(self.get_auth_urls())

    def resolve_url(self, to):
        if isinstance(to, Promise):
            to = str(to)

        if to is None:
            return None

        args, kwargs = None, None
        if not isinstance(to, str):
            if len(to) > 2:
                to, args, kwargs = to
            elif len(to) == 2:
                to, kwargs = to

        args = args or ()
        kwargs = kwargs or {}

        return resolve_url(to, *args, **kwargs)

    def get_auth_urls(self):
        urls = {
            "LOGIN_URL": self.resolve_url(swagger_settings.LOGIN_URL),
            "LOGOUT_URL": self.resolve_url(swagger_settings.LOGOUT_URL),
        }

        return filter_none(urls)

    def get_oauth2_config(self):
        data = swagger_settings.OAUTH2_CONFIG
        assert isinstance(data, dict), "OAUTH2_CONFIG must be a dict"
        return data


class SwaggerUIRenderer(_UIRenderer):
    """Renders a swagger-ui web interface for schema browsing."""

    template = "drf-yasg/swagger-ui.html"
    format = "swagger"

    def set_context(self, renderer_context, swagger=None):
        super(SwaggerUIRenderer, self).set_context(renderer_context, swagger)
        swagger_ui_settings = self.get_swagger_ui_settings()

        request = renderer_context.get("request", None)
        oauth_redirect_url = force_str(swagger_ui_settings.get("oauth2RedirectUrl", ""))
        if request and oauth_redirect_url:
            swagger_ui_settings["oauth2RedirectUrl"] = request.build_absolute_uri(
                oauth_redirect_url
            )

        renderer_context["swagger_settings"] = json.dumps(
            swagger_ui_settings, cls=encoders.JSONEncoder
        )

    def get_swagger_ui_settings(self):
        data = {
            "url": self.resolve_url(swagger_settings.SPEC_URL),
            "operationsSorter": swagger_settings.OPERATIONS_SORTER,
            "tagsSorter": swagger_settings.TAGS_SORTER,
            "docExpansion": swagger_settings.DOC_EXPANSION,
            "deepLinking": swagger_settings.DEEP_LINKING,
            "showExtensions": swagger_settings.SHOW_EXTENSIONS,
            "defaultModelRendering": swagger_settings.DEFAULT_MODEL_RENDERING,
            "defaultModelExpandDepth": swagger_settings.DEFAULT_MODEL_DEPTH,
            "defaultModelsExpandDepth": swagger_settings.DEFAULT_MODEL_DEPTH,
            "showCommonExtensions": swagger_settings.SHOW_COMMON_EXTENSIONS,
            "oauth2RedirectUrl": swagger_settings.OAUTH2_REDIRECT_URL,
            "supportedSubmitMethods": swagger_settings.SUPPORTED_SUBMIT_METHODS,
            "displayOperationId": swagger_settings.DISPLAY_OPERATION_ID,
            "persistAuth": swagger_settings.PERSIST_AUTH,
            "refetchWithAuth": swagger_settings.REFETCH_SCHEMA_WITH_AUTH,
            "refetchOnLogout": swagger_settings.REFETCH_SCHEMA_ON_LOGOUT,
            "fetchSchemaWithQuery": swagger_settings.FETCH_SCHEMA_WITH_QUERY,
            "csrfCookie": swagger_settings.CSRF_COOKIE_NAME,
            # remove HTTP_ and convert underscores to dashes
            "csrfHeader": swagger_settings.CSRF_HEADER_NAME[5:].replace("_", "-"),
        }

        data = filter_none(data)
        if swagger_settings.VALIDATOR_URL != "":
            data["validatorUrl"] = self.resolve_url(swagger_settings.VALIDATOR_URL)

        return data


class ReDocRenderer(_UIRenderer):
    """Renders a ReDoc web interface for schema browsing."""

    template = "drf-yasg/redoc.html"
    format = "redoc"

    def set_context(self, renderer_context, swagger=None):
        super(ReDocRenderer, self).set_context(renderer_context, swagger)
        renderer_context["redoc_settings"] = json.dumps(
            self.get_redoc_settings(), cls=encoders.JSONEncoder
        )

    def get_redoc_settings(self):
        data = {
            "url": self.resolve_url(redoc_settings.SPEC_URL),
            "lazyRendering": redoc_settings.LAZY_RENDERING,
            "hideHostname": redoc_settings.HIDE_HOSTNAME,
            "expandResponses": redoc_settings.EXPAND_RESPONSES,
            "pathInMiddlePanel": redoc_settings.PATH_IN_MIDDLE,
            "nativeScrollbars": redoc_settings.NATIVE_SCROLLBARS,
            "requiredPropsFirst": redoc_settings.REQUIRED_PROPS_FIRST,
            "fetchSchemaWithQuery": redoc_settings.FETCH_SCHEMA_WITH_QUERY,
            "hideDownloadButton": redoc_settings.HIDE_DOWNLOAD_BUTTON,
        }

        return filter_none(data)


class ReDocOldRenderer(ReDocRenderer):
    """Renders a ReDoc 1.x.x web interface for schema browsing."""

    template = "drf-yasg/redoc-old.html"
