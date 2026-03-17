
from django.conf import settings
from django.contrib.sites.shortcuts import get_current_site
from django.urls import reverse

from .app_settings import api_settings


if 'allauth' in settings.INSTALLED_APPS:
    from allauth.account import app_settings as allauth_account_settings
    from allauth.account.adapter import get_adapter
    from allauth.account.forms import ResetPasswordForm as DefaultPasswordResetForm
    from allauth.account.forms import default_token_generator
    from allauth.account.utils import (
        filter_users_by_email,
        user_pk_to_url_str,
        user_username,
    )
    from allauth.utils import build_absolute_uri


def default_url_generator(request, user, temp_key):
    path = reverse(
        'password_reset_confirm',
        args=[user_pk_to_url_str(user), temp_key],
    )

    if api_settings.PASSWORD_RESET_USE_SITES_DOMAIN:
        url = build_absolute_uri(None, path)
    else:
        url = build_absolute_uri(request, path)

    url = url.replace('%3F', '?')

    return url


class AllAuthPasswordResetForm(DefaultPasswordResetForm):
    def clean_email(self):
        """
        Invalid email should not raise error, as this would leak users
        for unit test: test_password_reset_with_invalid_email
        """
        email = self.cleaned_data["email"]
        email = get_adapter().clean_email(email)
        self.users = filter_users_by_email(email, is_active=True)
        return self.cleaned_data["email"]

    def save(self, request, **kwargs):
        current_site = get_current_site(request)
        email = self.cleaned_data['email']
        token_generator = kwargs.get('token_generator', default_token_generator)

        for user in self.users:

            temp_key = token_generator.make_token(user)

            # save it to the password reset model
            # password_reset = PasswordReset(user=user, temp_key=temp_key)
            # password_reset.save()

            # send the password reset email
            url_generator = kwargs.get('url_generator', default_url_generator)
            url = url_generator(request, user, temp_key)
            uid = user_pk_to_url_str(user)

            context = {
                'current_site': current_site,
                'user': user,
                'password_reset_url': url,
                'request': request,
                'token': temp_key,
                'uid': uid,
            }
            if (
                getattr(allauth_account_settings, "LOGIN_METHODS", None) and  # noqa: W504
                allauth_account_settings.AuthenticationMethod.EMAIL not in allauth_account_settings.LOGIN_METHODS
            ):
                context['username'] = user_username(user)
            elif (
                allauth_account_settings.AUTHENTICATION_METHOD != allauth_account_settings.AuthenticationMethod.EMAIL
            ):
                # AUTHENTICATION_METHOD is deprecated
                context['username'] = user_username(user)
            get_adapter(request).send_mail(
                'account/email/password_reset_key', email, context
            )
        return self.cleaned_data['email']
