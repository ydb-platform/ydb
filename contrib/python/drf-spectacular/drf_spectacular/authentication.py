from django.conf import settings

from drf_spectacular.extensions import OpenApiAuthenticationExtension
from drf_spectacular.plumbing import build_bearer_security_scheme_object


class SessionScheme(OpenApiAuthenticationExtension):
    target_class = 'rest_framework.authentication.SessionAuthentication'
    name = 'cookieAuth'
    priority = -1

    def get_security_definition(self, auto_schema):
        return {
            'type': 'apiKey',
            'in': 'cookie',
            'name': settings.SESSION_COOKIE_NAME,
        }


class BasicScheme(OpenApiAuthenticationExtension):
    target_class = 'rest_framework.authentication.BasicAuthentication'
    name = 'basicAuth'
    priority = -1

    def get_security_definition(self, auto_schema):
        return {
            'type': 'http',
            'scheme': 'basic',
        }


class TokenScheme(OpenApiAuthenticationExtension):
    target_class = 'rest_framework.authentication.TokenAuthentication'
    name = 'tokenAuth'
    match_subclasses = True
    priority = -1

    def get_security_definition(self, auto_schema):
        return build_bearer_security_scheme_object(
            header_name='Authorization',
            token_prefix=self.target.keyword,
        )
