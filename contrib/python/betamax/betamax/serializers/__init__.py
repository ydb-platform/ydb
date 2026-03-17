# -*- coding: utf-8 -*-
from .base import BaseSerializer
from .json_serializer import JSONSerializer
from .proxy import SerializerProxy

serializer_registry = {}

_serializers = [JSONSerializer]
serializer_registry.update(dict((s.name, s()) for s in _serializers))
del _serializers

__all__ = ('BaseSerializer', 'JSONSerializer', 'SerializerProxy')
