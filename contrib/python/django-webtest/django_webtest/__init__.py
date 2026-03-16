# -*- coding: utf-8 -*-
import copy

from django.conf import settings
from django.test.signals import template_rendered
from django.core.handlers.wsgi import WSGIHandler
from django.test import TestCase, TransactionTestCase
from django.test.client import store_rendered_templates

from functools import partial

try:
    from importlib import import_module
except ImportError:
    from django.utils.importlib import import_module

from django.core import signals
try:
    from django.db import close_old_connections
except ImportError:
    from django.db import close_connection
    close_old_connections = None
try:
    from django.core.servers.basehttp import (
            AdminMediaHandler as StaticFilesHandler)
except ImportError:
    from django.contrib.staticfiles.handlers import StaticFilesHandler

from webtest import TestApp
try:
    from webtest.utils import NoDefault
except ImportError:
    NoDefault = ''

from django_webtest.response import DjangoWebtestResponse
from django_webtest.compat import to_string, to_wsgi_safe_string


# sentinel to differentiate user=None / user param not given
_notgiven = object()


class DjangoTestApp(TestApp):
    response_class = DjangoWebtestResponse

    def __init__(self, *args, **kwargs):
        extra_environ = (kwargs.get('extra_environ') or {}).copy()
        extra_environ.setdefault('HTTP_HOST', 'testserver')
        kwargs['extra_environ'] = extra_environ
        super(DjangoTestApp, self).__init__(self.get_wsgi_handler(), *args, **kwargs)

    def get_wsgi_handler(self):
        return StaticFilesHandler(WSGIHandler())

    def set_user(self, user):
        """Update the user used by the app globall; pass None to unset."""
        if user is None and 'WEBTEST_USER' in self.extra_environ:
            del self.extra_environ['WEBTEST_USER']
        if user is not None:
            self.extra_environ = self._update_environ(self.extra_environ, user)

    def _update_environ(self, environ, user=_notgiven):
        environ = environ or {}

        if user is not _notgiven:
            if user is None:
                # We can't just delete the key here, the test request is built
                # from self.extra_environ + this environ, so the header defined
                # by set_user will be found in self.extra_environ.
                environ['WEBTEST_USER'] = ''
            else:
                username = _get_username(user)
                environ['WEBTEST_USER'] = to_wsgi_safe_string(username)

        return environ

    def do_request(self, req, status, expect_errors):
        # Django closes the database connection after every request;
        # this breaks the use of transactions in your tests.
        if close_old_connections is not None:  # Django 1.6+
            signals.request_started.disconnect(close_old_connections)
            signals.request_finished.disconnect(close_old_connections)
        else:  # Django < 1.6
            signals.request_finished.disconnect(close_connection)

        try:
            req.environ.setdefault('REMOTE_ADDR', '127.0.0.1')

            # is this a workaround for
            # https://code.djangoproject.com/ticket/11111 ?
            req.environ['REMOTE_ADDR'] = to_string(req.environ['REMOTE_ADDR'])
            req.environ['PATH_INFO'] = to_string(req.environ['PATH_INFO'])

            # Curry a data dictionary into an instance of the template renderer
            # callback function.
            data = {}
            on_template_render = partial(store_rendered_templates, data)
            template_rendered.connect(on_template_render)

            response = super(DjangoTestApp, self).do_request(req, status,
                                                             expect_errors)

            # Add any rendered template detail to the response.
            # If there was only one template rendered (the most likely case),
            # flatten the list to a single element.
            def flattend(detail):
                if len(data[detail]) == 1:
                    return data[detail][0]
                return data[detail]

            response.context = None
            response.template = None
            response.templates = data.get('templates', None)

            if data.get('context'):
                response.context = flattend('context')

            if data.get('template'):
                response.template = flattend('template')
            elif data.get('templates'):
                response.template = flattend('templates')

            response.__class__ = self.response_class
            return response
        finally:
            if close_old_connections:  # Django 1.6+
                signals.request_started.connect(close_old_connections)
                signals.request_finished.connect(close_old_connections)
            else:  # Django < 1.6
                signals.request_finished.connect(close_connection)

    def get(self, url, *args, **kwargs):
        extra_environ = kwargs.get('extra_environ')
        user = kwargs.pop('user', _notgiven)
        auto_follow = kwargs.pop('auto_follow', False)

        kwargs['extra_environ'] = self._update_environ(extra_environ, user)
        response = super(DjangoTestApp, self).get(url, *args, **kwargs)

        def is_redirect(r):
            return r.status_int >= 300 and r.status_int < 400
        while auto_follow and is_redirect(response):
            response = response.follow(**kwargs)

        return response

    def post(self, url, *args, **kwargs):
        extra_environ = kwargs.get('extra_environ')
        user = kwargs.pop('user', _notgiven)
        kwargs['extra_environ'] = self._update_environ(extra_environ, user)
        return super(DjangoTestApp, self).post(url, *args, **kwargs)

    def put(self, url, *args, **kwargs):
        extra_environ = kwargs.get('extra_environ')
        user = kwargs.pop('user', _notgiven)
        kwargs['extra_environ'] = self._update_environ(extra_environ, user)
        return super(DjangoTestApp, self).put(url, *args, **kwargs)

    def patch(self, url, *args, **kwargs):
        extra_environ = kwargs.get('extra_environ')
        user = kwargs.pop('user', _notgiven)
        kwargs['extra_environ'] = self._update_environ(extra_environ, user)
        return super(DjangoTestApp, self).patch(url, *args, **kwargs)

    def head(self, url, *args, **kwargs):
        extra_environ = kwargs.get('extra_environ')
        user = kwargs.pop('user', _notgiven)
        kwargs['extra_environ'] = self._update_environ(extra_environ, user)
        return super(DjangoTestApp, self).head(url, *args, **kwargs)

    def options(self, url, *args, **kwargs):
        extra_environ = kwargs.get('extra_environ')
        user = kwargs.pop('user', _notgiven)
        kwargs['extra_environ'] = self._update_environ(extra_environ, user)
        return super(DjangoTestApp, self).options(url, *args, **kwargs)

    def delete(self, url, *args, **kwargs):
        extra_environ = kwargs.get('extra_environ')
        user = kwargs.pop('user', _notgiven)
        kwargs['extra_environ'] = self._update_environ(extra_environ, user)
        return super(DjangoTestApp, self).delete(url, *args, **kwargs)

    def post_json(self, url, *args, **kwargs):
        extra_environ = kwargs.get('extra_environ')
        user = kwargs.pop('user', _notgiven)
        kwargs['extra_environ'] = self._update_environ(extra_environ, user)
        return super(DjangoTestApp, self).post_json(url, *args, **kwargs)

    def put_json(self, url, *args, **kwargs):
        extra_environ = kwargs.get('extra_environ')
        user = kwargs.pop('user', _notgiven)
        kwargs['extra_environ'] = self._update_environ(extra_environ, user)
        return super(DjangoTestApp, self).put_json(url, *args, **kwargs)

    def patch_json(self, url, *args, **kwargs):
        extra_environ = kwargs.get('extra_environ')
        user = kwargs.pop('user', _notgiven)
        kwargs['extra_environ'] = self._update_environ(extra_environ, user)
        return super(DjangoTestApp, self).patch_json(url, *args, **kwargs)

    def delete_json(self, url, *args, **kwargs):
        extra_environ = kwargs.get('extra_environ')
        user = kwargs.pop('user', _notgiven)
        kwargs['extra_environ'] = self._update_environ(extra_environ, user)
        return super(DjangoTestApp, self).delete_json(url, *args, **kwargs)

    @property
    def session(self):
        """
        Obtains the current session variables.
        """
        engine = import_module(settings.SESSION_ENGINE)
        cookie = self.cookies.get(settings.SESSION_COOKIE_NAME, None)
        if cookie:
            return engine.SessionStore(cookie)
        else:
            return {}

    def set_cookie(self, *args, **kwargs):
        self.extra_environ = self._update_environ(self.extra_environ)
        return super(DjangoTestApp, self).set_cookie(*args, **kwargs)


class WebTestMixin(object):
    extra_environ = {}
    csrf_checks = True
    setup_auth = True
    app_class = DjangoTestApp

    def _patch_settings(self):
        '''
        Patches settings to add support for django-webtest authorization
        and (optional) to disable CSRF checks.
        '''
        self._DEBUG_PROPAGATE_EXCEPTIONS = settings.DEBUG_PROPAGATE_EXCEPTIONS
        self._MIDDLEWARE = self.settings_middleware[:]
        self._AUTHENTICATION_BACKENDS = settings.AUTHENTICATION_BACKENDS[:]
        self._REST_FRAMEWORK = getattr(
            settings, 'REST_FRAMEWORK', {'DEFAULT_AUTHENTICATION_CLASSES': []})

        self.settings_middleware = list(self.settings_middleware)
        settings.AUTHENTICATION_BACKENDS = list(
            settings.AUTHENTICATION_BACKENDS)
        settings.REST_FRAMEWORK = copy.deepcopy(self._REST_FRAMEWORK)
        settings.DEBUG_PROPAGATE_EXCEPTIONS = True

        if not self.csrf_checks:
            self._disable_csrf_checks()

        if self.setup_auth:
            self._setup_auth()

    def _unpatch_settings(self):
        ''' Restores settings to before-patching state '''
        self.settings_middleware = self._MIDDLEWARE
        settings.AUTHENTICATION_BACKENDS = self._AUTHENTICATION_BACKENDS
        settings.DEBUG_PROPAGATE_EXCEPTIONS = self._DEBUG_PROPAGATE_EXCEPTIONS
        settings.REST_FRAMEWORK = self._REST_FRAMEWORK

    def _setup_auth(self):
        ''' Setups django-webtest authorization '''
        self._setup_auth_middleware()
        self._setup_auth_backend()
        self._setup_auth_class()

    def _disable_csrf_checks(self):
        disable_csrf_middleware = (
            'django_webtest.middleware.DisableCSRFCheckMiddleware')
        if disable_csrf_middleware not in self.settings_middleware:
            self.settings_middleware.insert(0, disable_csrf_middleware)

    def _setup_auth_middleware(self):
        webtest_auth_middleware = (
            'django_webtest.middleware.WebtestUserMiddleware')
        django_auth_middleware = (
            'django.contrib.auth.middleware.AuthenticationMiddleware')

        if django_auth_middleware not in self.settings_middleware:
            # There can be a custom AuthenticationMiddleware subclass or
            # replacement, we can't compute its index so just put our auth
            # middleware to the end.  If appending causes problems
            # _setup_auth_middleware method can be overriden by a subclass.
            self.settings_middleware.append(webtest_auth_middleware)
        elif webtest_auth_middleware not in self.settings_middleware:
            index = self.settings_middleware.index(django_auth_middleware)
            self.settings_middleware.insert(index + 1, webtest_auth_middleware)

    def _setup_auth_backend(self):
        backend_name = getattr(
            settings, 'WEBTEST_AUTHENTICATION_BACKEND',
            'django_webtest.backends.WebtestUserBackend')
        settings.AUTHENTICATION_BACKENDS.insert(0, backend_name)

    def _setup_auth_class(self):
        class_name = 'django_webtest.rest_framework_auth.WebtestAuthentication'
        drf_settings = settings.REST_FRAMEWORK
        try:
            classes = drf_settings['DEFAULT_AUTHENTICATION_CLASSES']
        except KeyError:
            classes = []
        if class_name not in classes:
            if isinstance(classes, tuple):
                classes = list(classes)
            classes.append(class_name)
            drf_settings['DEFAULT_AUTHENTICATION_CLASSES'] = classes

    @property
    def middleware_setting_name(self):
        try:
            return self._middleware_setting_name
        except AttributeError:
            if getattr(settings, 'MIDDLEWARE', None) is not None:
                name = 'MIDDLEWARE'
            else:
                name = 'MIDDLEWARE_CLASSES'
            self._middleware_setting_name = name
            return name

    @property
    def settings_middleware(self):
        return getattr(settings, self.middleware_setting_name)

    @settings_middleware.setter
    def settings_middleware(self, value):
        setattr(settings, self.middleware_setting_name, value)

    def renew_app(self):
        """
        Resets self.app (drops the stored state): cookies, etc.
        Note: this renews only self.app, not the responses fetched by self.app.
        """
        self.app = self.app_class(extra_environ=self.extra_environ)

    def __call__(self, result=None):
        self._patch_settings()
        self.renew_app()
        res = super(WebTestMixin, self).__call__(result)
        self._unpatch_settings()
        return res


class WebTest(WebTestMixin, TestCase):
    pass


class TransactionWebTest(WebTestMixin, TransactionTestCase):
    pass


def _get_username(user):
    """
    Return user's username. ``user`` can be standard Django User
    instance, a custom user model or just an username (as string).
    """
    value = None
    # custom user, django 1.5+
    get_username = getattr(user, 'get_username', None)
    if get_username is not None:
        value = get_username()
    if value is None:
        # standard User
        username = getattr(user, 'username', None)
        if username is not None:
            value = username
        else:
            # assume user is just an username
            value = user
    return value
