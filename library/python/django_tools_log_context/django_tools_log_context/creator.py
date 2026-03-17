# coding: utf-8

from __future__ import unicode_literals

import logging
from django.conf import settings
from importlib import import_module
from cached_property import cached_property

log = logging.getLogger(__name__)


class Creator(object):
    def _providers_import_paths(self):
        return settings.TOOLS_LOG_CONTEXT_PROVIDERS

    def _resolve(self, import_path):
        try:
            module = import_module(import_path)
        except ImportError:
            log.exception('cannot import "%s"', import_path)
            return None
        try:
            the_callable = getattr(module, 'Provider')
        except AttributeError:
            log.exception('no "Provider" class in "%s"', import_path)
            return None
        if not callable(the_callable):
            log.error('"%s" is not callable', import_path)
            return None
        return the_callable

    @cached_property
    def providers(self):
        providers = []
        for path in self._providers_import_paths():
            provider = self._resolve(path)
            if provider:
                providers.append(provider())
        return providers

    def validate_kwargs(self, kwargs):
        required_params = set()
        for provider in self.providers:
            required_params.update(set(provider.required_kwargs))
        diff = required_params - set(kwargs)
        if diff:
            raise KeyError('missing required {0}'.format(','.join(diff)))

    def do(self, **kwargs):
        ctx = {}
        self.validate_kwargs(kwargs)
        for provider in self.providers:
            ctx.update(provider(**kwargs))
        return ctx
