#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    flask_humanize.py
    ~~~~~~~~~~~~~~~~~

    Add common humanization utilities, like turning number into a fuzzy
    human-readable duration or into human-readable size, to your flask
    applications.

    :copyright: (c) by Vital Kudzelka
    :license: MIT
"""
from datetime import datetime
import humanize
from flask import current_app
from werkzeug.datastructures import ImmutableDict

from .compat import (
    text_type, string_types
)


__version__ = '0.3.0'


def force_unicode(value):
    if isinstance(value, string_types):
        if not isinstance(value, text_type):
            return value.decode('utf-8')
    return value


def app_has_babel(app):
    """Check application instance for configured babel extension."""
    obj = app.extensions.get('babel')
    return obj is not None


def self_name(string):
    """Create config key for extension."""
    return 'HUMANIZE_{0}'.format(string.upper())


default_config = ImmutableDict({
    # The default locale to work with. When `BABEL_DEFAULT_LOCALE` is
    # available then it used instead.
    'default_locale': 'en',

    # Use UTC instead of local time for humanize dates and times.
    'use_utc': False,
})


class Humanize(object):
    """Add common humanization utilities, like turning number into a fuzzy
    human-readable duration or into human-readable size, to your flask
    applications.
    """

    # A function uses for locale selection.
    locale_selector_func = None

    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """Initialize application to use with extension.

        :param app: The Flask instance.

        Example::

            from myapp import create_app()
            from flask_humanize import Humanize

            app = create_app()
            humanize.init_app(app)

        """
        for k, v in default_config.items():
            app.config.setdefault(self_name(k), v)

        if app_has_babel(app):
            default_locale = app.config['BABEL_DEFAULT_LOCALE']
            if default_locale is not None:
                app.config.setdefault(self_name('default_locale'), default_locale)

        if app.config.get('HUMANIZE_USE_UTC'):
            # Override humanize.time._now to use UTC time
            humanize.time._now = datetime.utcnow
        else:
            humanize.time._now = datetime.now

        app.add_template_filter(self._humanize, 'humanize')
        app.before_request(self._set_locale)
        app.after_request(self._unset_locale)

        if not hasattr(app, 'extensions'):
            app.extensions = {}
        app.extensions['humanize'] = self

    @property
    def default_locale(self):
        """Returns the default locale for current application."""
        return current_app.config['HUMANIZE_DEFAULT_LOCALE']

    def localeselector(self, func):
        """Registers a callback function for locale selection.

        Callback is a callable which returns the locale as string,
        e.g. ``'en_US'``, ``'ru_RU'``::

            from flask import request

            @humanize.localeselector
            def get_locale():
                return request.accept_languages.best_match(available_locales)

        When no callback is available or `None` is returned, then locale
        falls back to the default one from application configuration.
        """
        self.locale_selector_func = func
        return func

    def _set_locale(self):
        if self.locale_selector_func is None:
            locale = self.default_locale
        else:
            locale = self.locale_selector_func()
            if locale is None:
                locale = self.default_locale

        try:
            humanize.i18n.activate(locale)
        except IOError:
            pass

    def _unset_locale(self, response):
        humanize.i18n.deactivate()
        return response

    def _humanize(self, value, fname='naturaltime', **kwargs):
        try:
            method = getattr(humanize, fname)
        except AttributeError:
            raise Exception(
                "Humanize module does not contains function '%s'" % fname
            )

        try:
            value = method(value, **kwargs) if kwargs else method(value)
        except Exception:
            raise ValueError(
                "An error occured during execution function '%s'" % fname
            )

        return force_unicode(value)
