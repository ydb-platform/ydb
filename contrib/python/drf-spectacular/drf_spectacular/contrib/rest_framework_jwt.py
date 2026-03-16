from drf_spectacular.extensions import OpenApiAuthenticationExtension
from drf_spectacular.plumbing import build_bearer_security_scheme_object


class JWTScheme(OpenApiAuthenticationExtension):
    target_class = 'rest_framework_jwt.authentication.JSONWebTokenAuthentication'
    name = 'jwtAuth'

    def get_security_definition(self, auto_schema):
        from rest_framework_jwt.settings import api_settings

        return build_bearer_security_scheme_object(
            header_name='AUTHORIZATION',
            token_prefix=api_settings.JWT_AUTH_HEADER_PREFIX,
            bearer_format='JWT'
        )
