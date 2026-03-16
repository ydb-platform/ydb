from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from rest_framework import status
from rest_framework import exceptions, serializers
from rest_framework.authentication import CSRFCheck
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework_simplejwt.serializers import TokenRefreshSerializer

from .app_settings import api_settings


def set_jwt_access_cookie(response, access_token):
    from rest_framework_simplejwt.settings import api_settings as jwt_settings
    cookie_name = api_settings.JWT_AUTH_COOKIE
    access_token_expiration = (timezone.now() + jwt_settings.ACCESS_TOKEN_LIFETIME)
    cookie_secure = api_settings.JWT_AUTH_SECURE
    cookie_httponly = api_settings.JWT_AUTH_HTTPONLY
    cookie_samesite = api_settings.JWT_AUTH_SAMESITE
    cookie_domain = api_settings.JWT_AUTH_COOKIE_DOMAIN

    if cookie_name:
        response.set_cookie(
            cookie_name,
            access_token,
            expires=access_token_expiration,
            secure=cookie_secure,
            httponly=cookie_httponly,
            samesite=cookie_samesite,
            domain=cookie_domain,
        )


def set_jwt_refresh_cookie(response, refresh_token):
    from rest_framework_simplejwt.settings import api_settings as jwt_settings
    refresh_token_expiration = (timezone.now() + jwt_settings.REFRESH_TOKEN_LIFETIME)
    refresh_cookie_name = api_settings.JWT_AUTH_REFRESH_COOKIE
    refresh_cookie_path = api_settings.JWT_AUTH_REFRESH_COOKIE_PATH
    cookie_secure = api_settings.JWT_AUTH_SECURE
    cookie_httponly = api_settings.JWT_AUTH_HTTPONLY
    cookie_samesite = api_settings.JWT_AUTH_SAMESITE
    cookie_domain = api_settings.JWT_AUTH_COOKIE_DOMAIN

    if refresh_cookie_name:
        response.set_cookie(
            refresh_cookie_name,
            refresh_token,
            expires=refresh_token_expiration,
            secure=cookie_secure,
            httponly=cookie_httponly,
            samesite=cookie_samesite,
            path=refresh_cookie_path,
            domain=cookie_domain,
        )


def set_jwt_cookies(response, access_token, refresh_token):
    set_jwt_access_cookie(response, access_token)
    set_jwt_refresh_cookie(response, refresh_token)


def unset_jwt_cookies(response):
    cookie_name = api_settings.JWT_AUTH_COOKIE
    refresh_cookie_name = api_settings.JWT_AUTH_REFRESH_COOKIE
    refresh_cookie_path = api_settings.JWT_AUTH_REFRESH_COOKIE_PATH
    cookie_samesite = api_settings.JWT_AUTH_SAMESITE
    cookie_domain = api_settings.JWT_AUTH_COOKIE_DOMAIN

    if cookie_name:
        response.delete_cookie(cookie_name, samesite=cookie_samesite, domain=cookie_domain)
    if refresh_cookie_name:
        response.delete_cookie(refresh_cookie_name, path=refresh_cookie_path, samesite=cookie_samesite, domain=cookie_domain)


class CookieTokenRefreshSerializer(TokenRefreshSerializer):
    refresh = serializers.CharField(required=False, help_text=_('WIll override cookie.'))

    def extract_refresh_token(self):
        request = self.context['request']
        if 'refresh' in request.data and request.data['refresh'] != '':
            return request.data['refresh']
        cookie_name = api_settings.JWT_AUTH_REFRESH_COOKIE
        if cookie_name and cookie_name in request.COOKIES:
            return request.COOKIES.get(cookie_name)
        else:
            from rest_framework_simplejwt.exceptions import InvalidToken
            raise InvalidToken(_('No valid refresh token found.'))

    def validate(self, attrs):
        attrs['refresh'] = self.extract_refresh_token()
        return super().validate(attrs)


def get_refresh_view():
    """ Returns a Token Refresh CBV without a circular import """
    from rest_framework_simplejwt.settings import api_settings as jwt_settings
    from rest_framework_simplejwt.views import TokenRefreshView

    class RefreshViewWithCookieSupport(TokenRefreshView):
        serializer_class = CookieTokenRefreshSerializer

        def finalize_response(self, request, response, *args, **kwargs):
            if response.status_code == status.HTTP_200_OK and 'access' in response.data:
                set_jwt_access_cookie(response, response.data['access'])
                response.data['access_expiration'] = (timezone.now() + jwt_settings.ACCESS_TOKEN_LIFETIME)
            if response.status_code == status.HTTP_200_OK and 'refresh' in response.data:
                set_jwt_refresh_cookie(response, response.data['refresh'])
                if api_settings.JWT_AUTH_HTTPONLY:
                    del response.data['refresh']
                else:
                    response.data['refresh_expiration'] = (timezone.now() + jwt_settings.REFRESH_TOKEN_LIFETIME)
            return super().finalize_response(request, response, *args, **kwargs)
    return RefreshViewWithCookieSupport


class JWTCookieAuthentication(JWTAuthentication):
    """
    An authentication plugin that hopefully authenticates requests through a JSON web
    token provided in a request cookie (and through the header as normal, with a
    preference to the header).
    """
    def enforce_csrf(self, request):
        """
        Enforce CSRF validation for session based authentication.
        """
        def dummy_get_response(request):  # pragma: no cover
            return None
        check = CSRFCheck(dummy_get_response)
        # populates request.META['CSRF_COOKIE'], which is used in process_view()
        check.process_request(request)
        reason = check.process_view(request, None, (), {})
        if reason:
            # CSRF failed, bail with explicit error message
            raise exceptions.PermissionDenied(f'CSRF Failed: {reason}')

    def authenticate(self, request):
        cookie_name = api_settings.JWT_AUTH_COOKIE
        header = self.get_header(request)
        if header is None:
            if cookie_name:
                raw_token = request.COOKIES.get(cookie_name)
                if api_settings.JWT_AUTH_COOKIE_ENFORCE_CSRF_ON_UNAUTHENTICATED:  # True at your own risk
                    self.enforce_csrf(request)
                elif raw_token is not None and api_settings.JWT_AUTH_COOKIE_USE_CSRF:
                    self.enforce_csrf(request)
            else:
                return None
        else:
            raw_token = self.get_raw_token(header)

        if raw_token is None:
            return None

        validated_token = self.get_validated_token(raw_token)
        return self.get_user(validated_token), validated_token
