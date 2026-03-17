from django.urls import re_path

from dj_rest_auth.app_settings import api_settings

from dj_rest_auth.views import (
    LoginView, LogoutView, PasswordChangeView, PasswordResetConfirmView,
    PasswordResetView, UserDetailsView,
)


urlpatterns = [
    # URLs that do not require a session or valid token
    re_path(r'password/reset/?$', PasswordResetView.as_view(), name='rest_password_reset'),
    re_path(r'password/reset/confirm/?$', PasswordResetConfirmView.as_view(), name='rest_password_reset_confirm'),
    re_path(r'login/?$', LoginView.as_view(), name='rest_login'),
    # URLs that require a user to be logged in with a valid session / token.
    re_path(r'logout/?$', LogoutView.as_view(), name='rest_logout'),
    re_path(r'user/?$', UserDetailsView.as_view(), name='rest_user_details'),
    re_path(r'password/change/?$', PasswordChangeView.as_view(), name='rest_password_change'),
]

if api_settings.USE_JWT:
    from rest_framework_simplejwt.views import TokenVerifyView

    from dj_rest_auth.jwt_auth import get_refresh_view

    urlpatterns += [
        re_path(r'token/verify/?$', TokenVerifyView.as_view(), name='token_verify'),
        re_path(r'token/refresh/?$', get_refresh_view().as_view(), name='token_refresh'),
    ]
