# -*- coding: utf-8 -*-
from django_webtest import WebTestMixin
import pytest


class MixinWithInstanceVariables(WebTestMixin):
    """
    Override WebTestMixin to make all of its variables instance variables
    not class variables; otherwise multiple django_app_factory fixtures contend
    for the same class variables
    """
    def __init__(self):
        self.extra_environ = {}
        self.csrf_checks = True
        self.setup_auth = True


@pytest.fixture(scope='session')
def django_app_mixin():
    app_mixin = MixinWithInstanceVariables()
    return app_mixin


@pytest.fixture
def django_app(django_app_mixin):
    django_app_mixin._patch_settings()
    django_app_mixin.renew_app()
    yield django_app_mixin.app
    django_app_mixin._unpatch_settings()


@pytest.fixture
def django_app_factory():
    app_mixin = MixinWithInstanceVariables()

    def factory(csrf_checks=True, extra_environ=None):
        app_mixin.csrf_checks = csrf_checks
        if extra_environ:
            app_mixin.extra_environ = extra_environ
        app_mixin._patch_settings()
        app_mixin.renew_app()
        return app_mixin.app

    yield factory
    
    app_mixin._unpatch_settings()
