from rest_framework import serializers

from drf_spectacular.drainage import warn
from drf_spectacular.extensions import OpenApiAuthenticationExtension, OpenApiSerializerExtension
from drf_spectacular.plumbing import build_bearer_security_scheme_object
from drf_spectacular.utils import inline_serializer


class TokenObtainPairSerializerExtension(OpenApiSerializerExtension):
    target_class = 'rest_framework_simplejwt.serializers.TokenObtainPairSerializer'

    def map_serializer(self, auto_schema, direction):
        Fixed = inline_serializer('Fixed', fields={
            self.target_class.username_field: serializers.CharField(write_only=True),
            'password': serializers.CharField(write_only=True),
            'access': serializers.CharField(read_only=True),
            'refresh': serializers.CharField(read_only=True),
        })
        return auto_schema._map_serializer(Fixed, direction)


class TokenObtainSlidingSerializerExtension(OpenApiSerializerExtension):
    target_class = 'rest_framework_simplejwt.serializers.TokenObtainSlidingSerializer'

    def map_serializer(self, auto_schema, direction):
        Fixed = inline_serializer('Fixed', fields={
            self.target_class.username_field: serializers.CharField(write_only=True),
            'password': serializers.CharField(write_only=True),
            'token': serializers.CharField(read_only=True),
        })
        return auto_schema._map_serializer(Fixed, direction)


class TokenRefreshSerializerExtension(OpenApiSerializerExtension):
    target_class = 'rest_framework_simplejwt.serializers.TokenRefreshSerializer'

    def map_serializer(self, auto_schema, direction):
        from rest_framework_simplejwt.settings import api_settings

        if api_settings.ROTATE_REFRESH_TOKENS:
            class Fixed(serializers.Serializer):
                access = serializers.CharField(read_only=True)
                refresh = serializers.CharField()
        else:
            class Fixed(serializers.Serializer):
                access = serializers.CharField(read_only=True)
                refresh = serializers.CharField(write_only=True)

        return auto_schema._map_serializer(Fixed, direction)


class TokenVerifySerializerExtension(OpenApiSerializerExtension):
    target_class = 'rest_framework_simplejwt.serializers.TokenVerifySerializer'

    def map_serializer(self, auto_schema, direction):
        Fixed = inline_serializer('Fixed', fields={
            'token': serializers.CharField(write_only=True),
        })
        return auto_schema._map_serializer(Fixed, direction)


class SimpleJWTScheme(OpenApiAuthenticationExtension):
    target_class = 'rest_framework_simplejwt.authentication.JWTAuthentication'
    name = 'jwtAuth'

    def get_security_definition(self, auto_schema):
        from rest_framework_simplejwt.settings import api_settings

        if len(api_settings.AUTH_HEADER_TYPES) > 1:
            warn(
                f'OpenAPI3 can only have one "bearerFormat". JWT Settings specify '
                f'{api_settings.AUTH_HEADER_TYPES}. Using the first one.'
            )

        return build_bearer_security_scheme_object(
            header_name=getattr(api_settings, 'AUTH_HEADER_NAME', 'HTTP_AUTHORIZATION'),
            token_prefix=api_settings.AUTH_HEADER_TYPES[0],
            bearer_format='JWT'
        )


class SimpleJWTTokenUserScheme(SimpleJWTScheme):
    target_class = 'rest_framework_simplejwt.authentication.JWTTokenUserAuthentication'


class SimpleJWTStatelessUserScheme(SimpleJWTScheme):
    target_class = "rest_framework_simplejwt.authentication.JWTStatelessUserAuthentication"
