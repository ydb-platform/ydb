from django.conf import settings
from django.utils.translation import gettext_lazy as _
from rest_framework.settings import APISettings as _APISettings


USER_SETTINGS = getattr(settings, "REST_AUTH", None)

DEFAULTS = {
    'LOGIN_SERIALIZER': 'dj_rest_auth.serializers.LoginSerializer',
    'TOKEN_SERIALIZER': 'dj_rest_auth.serializers.TokenSerializer',
    'JWT_SERIALIZER': 'dj_rest_auth.serializers.JWTSerializer',
    'JWT_SERIALIZER_WITH_EXPIRATION': 'dj_rest_auth.serializers.JWTSerializerWithExpiration',
    'JWT_TOKEN_CLAIMS_SERIALIZER': 'rest_framework_simplejwt.serializers.TokenObtainPairSerializer',
    'USER_DETAILS_SERIALIZER': 'dj_rest_auth.serializers.UserDetailsSerializer',
    'PASSWORD_RESET_SERIALIZER': 'dj_rest_auth.serializers.PasswordResetSerializer',
    'PASSWORD_RESET_CONFIRM_SERIALIZER': 'dj_rest_auth.serializers.PasswordResetConfirmSerializer',
    'PASSWORD_CHANGE_SERIALIZER': 'dj_rest_auth.serializers.PasswordChangeSerializer',

    'REGISTER_SERIALIZER': 'dj_rest_auth.registration.serializers.RegisterSerializer',

    'REGISTER_PERMISSION_CLASSES': ('rest_framework.permissions.AllowAny',),

    'TOKEN_MODEL': 'rest_framework.authtoken.models.Token',
    'TOKEN_CREATOR': 'dj_rest_auth.utils.default_create_token',

    'PASSWORD_RESET_USE_SITES_DOMAIN': False,
    'OLD_PASSWORD_FIELD_ENABLED': False,
    'LOGOUT_ON_PASSWORD_CHANGE': False,
    'SESSION_LOGIN': True,
    'USE_JWT': False,

    'JWT_AUTH_COOKIE': None,
    'JWT_AUTH_REFRESH_COOKIE': None,
    'JWT_AUTH_REFRESH_COOKIE_PATH': '/',
    'JWT_AUTH_SECURE': False,
    'JWT_AUTH_HTTPONLY': True,
    'JWT_AUTH_SAMESITE': 'Lax',
    'JWT_AUTH_COOKIE_DOMAIN': None,
    'JWT_AUTH_RETURN_EXPIRATION': False,
    'JWT_AUTH_COOKIE_USE_CSRF': False,
    'JWT_AUTH_COOKIE_ENFORCE_CSRF_ON_UNAUTHENTICATED': False,
}

# List of settings that may be in string import notation.
IMPORT_STRINGS = (
    'TOKEN_CREATOR',
    'TOKEN_MODEL',
    'TOKEN_SERIALIZER',
    'JWT_SERIALIZER',
    'JWT_SERIALIZER_WITH_EXPIRATION',
    'JWT_TOKEN_CLAIMS_SERIALIZER',
    'USER_DETAILS_SERIALIZER',
    'LOGIN_SERIALIZER',
    'PASSWORD_RESET_SERIALIZER',
    'PASSWORD_RESET_CONFIRM_SERIALIZER',
    'PASSWORD_CHANGE_SERIALIZER',
    'REGISTER_SERIALIZER',
    'REGISTER_PERMISSION_CLASSES',
)

# List of settings that have been removed
REMOVED_SETTINGS = []


class APISettings(_APISettings):  # pragma: no cover
    def __check_user_settings(self, user_settings):
        from .utils import format_lazy
        SETTINGS_DOC = 'https://dj-rest-auth.readthedocs.io/en/latest/configuration.html'

        for setting in REMOVED_SETTINGS:
            if setting in user_settings:
                raise RuntimeError(
                    format_lazy(
                        _(
                            "The '{}' setting has been removed. Please refer to '{}' for available settings."
                        ),
                        setting,
                        SETTINGS_DOC,
                    )
                )

        return user_settings


api_settings = APISettings(USER_SETTINGS, DEFAULTS, IMPORT_STRINGS)
