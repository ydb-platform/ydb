# -*- coding: utf-8 -*-
# Copyright: See the LICENSE file.


"""Additional declarations for "faker" attributes.

Usage:

    class MyFactory(factory.Factory):
        class Meta:
            model = MyProfile

        first_name = factory.Faker('name')
"""


from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib

import faker
import faker.config

from . import declarations


class Faker(declarations.BaseDeclaration):
    """Wrapper for 'faker' values.

    Args:
        provider (str): the name of the Faker field
        locale (str): the locale to use for the faker

        All other kwargs will be passed to the underlying provider
        (e.g ``factory.Faker('ean', length=10)``
        calls ``faker.Faker.ean(length=10)``)

    Usage:
        >>> foo = factory.Faker('name')
    """
    def __init__(self, provider, **kwargs):
        super(Faker, self).__init__()
        self.provider = provider
        self.provider_kwargs = kwargs
        self.locale = kwargs.pop('locale', None)

    def generate(self, extra_kwargs=None):
        kwargs = {}
        kwargs.update(self.provider_kwargs)
        kwargs.update(extra_kwargs or {})
        subfaker = self._get_faker(self.locale)
        return subfaker.format(self.provider, **kwargs)

    def evaluate(self, instance, step, extra):
        return self.generate(extra)

    _FAKER_REGISTRY = {}
    _DEFAULT_LOCALE = faker.config.DEFAULT_LOCALE

    @classmethod
    @contextlib.contextmanager
    def override_default_locale(cls, locale):
        old_locale = cls._DEFAULT_LOCALE
        cls._DEFAULT_LOCALE = locale
        try:
            yield
        finally:
            cls._DEFAULT_LOCALE = old_locale

    @classmethod
    def _get_faker(cls, locale=None):
        if locale is None:
            locale = cls._DEFAULT_LOCALE

        if locale not in cls._FAKER_REGISTRY:
            subfaker = faker.Faker(locale=locale)
            cls._FAKER_REGISTRY[locale] = subfaker

        return cls._FAKER_REGISTRY[locale]

    @classmethod
    def add_provider(cls, provider, locale=None):
        """Add a new Faker provider for the specified locale"""
        cls._get_faker(locale).add_provider(provider)
