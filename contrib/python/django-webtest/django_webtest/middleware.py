# -*- coding: utf-8 -*-
import django
from django.contrib.auth.middleware import RemoteUserMiddleware
from django.core.exceptions import ImproperlyConfigured
from django.contrib import auth
if django.VERSION >= (1, 10):
    from django.utils.deprecation import MiddlewareMixin
else:
    MiddlewareMixin = object

from django_webtest.compat import is_authenticated


class WebtestUserMiddleware(RemoteUserMiddleware):
    """
    Middleware for utilizing django-webtest simplified auth
    ('user' arg for self.app.post and self.app.get).

    Mostly copied from RemoteUserMiddleware, but the auth backend is changed
    (by changing ``auth.authenticate`` arguments) in order to keep
    RemoteUser backend untouched during django-webtest auth.
    """

    header = "WEBTEST_USER"

    def __call__(self, request):
        # AuthenticationMiddleware is required so that request.user exists.
        if not hasattr(request, 'user'):
            raise ImproperlyConfigured(
                "The django-webtest auth middleware requires the "
                "'django.contrib.auth.middleware.AuthenticationMiddleware' "
                "to be installed. Add it to your MIDDLEWARE setting "
                "or disable django-webtest auth support by setting "
                "'setup_auth' property of your WebTest subclass to False."
            )
        try:
            username = request.META[self.header]
        except KeyError:
            # If specified header doesn't exist then return (leaving
            # request.user set to AnonymousUser by the
            # AuthenticationMiddleware).
            username = None
        if username is None:    
            return self.get_response(request)
        # If the user is already authenticated and that user is the user we are
        # getting passed in the headers, then the correct user is already
        # persisted in the session and we don't need to continue.
        if is_authenticated(request.user):
            if hasattr(request.user, "get_username"):
                authenticated_username = request.user.get_username()
            else:
                authenticated_username = request.user.username
            clean_username = self.clean_username(username, request)
            if authenticated_username == clean_username:
                return self.get_response(request)
        # We are seeing this user for the first time in this session, attempt
        # to authenticate the user.
        user = auth.authenticate(request=request, django_webtest_user=username)
        if user:
            # User is valid.  Set request.user and persist user in the session
            # by logging the user in.
            request.user = user
            auth.login(request, user)
        return self.get_response(request)


class DisableCSRFCheckMiddleware(MiddlewareMixin):
    def process_request(self, request):
        request._dont_enforce_csrf_checks = True
