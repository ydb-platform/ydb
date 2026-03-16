from django.conf import settings
from django.utils.version import get_version_tuple
from rest_framework import serializers

from drf_spectacular.contrib.rest_framework_simplejwt import (
    SimpleJWTScheme, TokenRefreshSerializerExtension,
)
from drf_spectacular.drainage import warn
from drf_spectacular.extensions import OpenApiSerializerExtension, OpenApiViewExtension
from drf_spectacular.utils import extend_schema


def get_dj_rest_auth_setting(class_name, setting_name):
    from dj_rest_auth.__version__ import __version__

    if get_version_tuple(__version__) < (3, 0, 0):
        from dj_rest_auth import app_settings

        return getattr(app_settings, class_name)
    else:
        from dj_rest_auth.app_settings import api_settings

        return getattr(api_settings, setting_name)


def get_use_jwt():
    from dj_rest_auth.__version__ import __version__

    if get_version_tuple(__version__) < (3, 0, 0):
        return getattr(settings, 'REST_USE_JWT', False)
    else:
        from dj_rest_auth.app_settings import api_settings

        return api_settings.USE_JWT


def get_token_serializer_class():
    if get_use_jwt():
        return get_dj_rest_auth_setting('JWTSerializer', 'JWT_SERIALIZER')
    else:
        return get_dj_rest_auth_setting('TokenSerializer', 'TOKEN_SERIALIZER')


class RestAuthDetailSerializer(serializers.Serializer):
    detail = serializers.CharField(read_only=True, required=False)


class RestAuthDefaultResponseView(OpenApiViewExtension):
    def view_replacement(self):
        class Fixed(self.target_class):
            @extend_schema(responses=RestAuthDetailSerializer)
            def post(self, request, *args, **kwargs):
                pass  # pragma: no cover

        return Fixed


class RestAuthLoginView(OpenApiViewExtension):
    target_class = 'dj_rest_auth.views.LoginView'

    def view_replacement(self):
        class Fixed(self.target_class):
            @extend_schema(responses=get_token_serializer_class())
            def post(self, request, *args, **kwargs):
                pass  # pragma: no cover

        return Fixed


class RestAuthLogoutView(OpenApiViewExtension):
    target_class = 'dj_rest_auth.views.LogoutView'

    def view_replacement(self):
        if getattr(settings, 'ACCOUNT_LOGOUT_ON_GET', None):
            get_schema_params = {'responses': RestAuthDetailSerializer}
        else:
            get_schema_params = {'exclude': True}

        if (
            get_use_jwt()
            and 'rest_framework_simplejwt.token_blacklist' in settings.INSTALLED_APPS
            and not get_dj_rest_auth_setting('JWT_AUTH_HTTPONLY', 'JWT_AUTH_HTTPONLY')
        ):
            class LogoutSerializer(serializers.Serializer):
                refresh = serializers.CharField(required=True, allow_blank=False)

            post_request_class = LogoutSerializer
        else:
            post_request_class = None

        class Fixed(self.target_class):
            @extend_schema(**get_schema_params)
            def get(self, request, *args, **kwargs):
                pass  # pragma: no cover

            @extend_schema(
                request=post_request_class, responses=RestAuthDetailSerializer
            )
            def post(self, request, *args, **kwargs):
                pass  # pragma: no cover

        return Fixed


class RestAuthPasswordChangeView(RestAuthDefaultResponseView):
    target_class = 'dj_rest_auth.views.PasswordChangeView'


class RestAuthPasswordResetView(RestAuthDefaultResponseView):
    target_class = 'dj_rest_auth.views.PasswordResetView'


class RestAuthPasswordResetConfirmView(RestAuthDefaultResponseView):
    target_class = 'dj_rest_auth.views.PasswordResetConfirmView'


class RestAuthVerifyEmailView(RestAuthDefaultResponseView):
    target_class = 'dj_rest_auth.registration.views.VerifyEmailView'
    optional = True


class RestAuthResendEmailVerificationView(RestAuthDefaultResponseView):
    target_class = 'dj_rest_auth.registration.views.ResendEmailVerificationView'
    optional = True


class RestAuthJWTSerializer(OpenApiSerializerExtension):
    target_class = 'dj_rest_auth.serializers.JWTSerializer'

    def map_serializer(self, auto_schema, direction):
        class Fixed(self.target_class):
            user = get_dj_rest_auth_setting('UserDetailsSerializer', 'USER_DETAILS_SERIALIZER')()

        return auto_schema._map_serializer(Fixed, direction)


class CookieTokenRefreshSerializerExtension(TokenRefreshSerializerExtension):
    target_class = 'dj_rest_auth.jwt_auth.CookieTokenRefreshSerializer'
    optional = True

    def get_name(self):
        return 'TokenRefresh'


class RestAuthRegisterView(OpenApiViewExtension):
    target_class = 'dj_rest_auth.registration.views.RegisterView'
    optional = True

    def view_replacement(self):
        from allauth.account.app_settings import EMAIL_VERIFICATION, EmailVerificationMethod

        if EMAIL_VERIFICATION == EmailVerificationMethod.MANDATORY:
            response_serializer = RestAuthDetailSerializer
        else:
            response_serializer = get_token_serializer_class()

        class Fixed(self.target_class):
            @extend_schema(responses=response_serializer)
            def post(self, request, *args, **kwargs):
                pass  # pragma: no cover

        return Fixed


class SimpleJWTCookieScheme(SimpleJWTScheme):
    target_class = 'dj_rest_auth.jwt_auth.JWTCookieAuthentication'
    optional = True
    name = ['jwtHeaderAuth', 'jwtCookieAuth']  # type: ignore

    def get_security_requirement(self, auto_schema):
        return [{name: []} for name in self.name]

    def get_security_definition(self, auto_schema):
        cookie_name = get_dj_rest_auth_setting('JWT_AUTH_COOKIE', 'JWT_AUTH_COOKIE')
        if not cookie_name:
            cookie_name = 'jwt-auth'
            warn(
                f'"JWT_AUTH_COOKIE" setting required for JWTCookieAuthentication. '
                f'defaulting to {cookie_name}'
            )

        return [
            super().get_security_definition(auto_schema),  # JWT from header
            {
                'type': 'apiKey',
                'in': 'cookie',
                'name': cookie_name,
            }
        ]
