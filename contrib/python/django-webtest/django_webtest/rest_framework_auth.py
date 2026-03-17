from django.contrib.auth import authenticate

from rest_framework import authentication


class WebtestAuthentication(authentication.BaseAuthentication):
    """Bridge between webtest and django-rest-framework."""

    header = 'WEBTEST_USER'

    def authenticate(self, request):
        value = request.META.get(self.header)
        if value:
            account = authenticate(django_webtest_user=value)
            if account and account.is_active:
                return (account, None)
