# coding: utf-8

from __future__ import unicode_literals

import uuid

from .base import BaseProvider


class Provider(BaseProvider):
    required_kwargs = ['endpoint']

    def endpoint(self, endpoint):
        if hasattr(endpoint, '__name__'):
            name = endpoint.__name__
        else:
            name = endpoint.__class__.__name__
        return {
            'name': name,
        }
