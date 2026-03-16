# -*- coding: utf-8 -*-

from __future__ import unicode_literals, absolute_import

import inspect
from collections import OrderedDict

from ..common import *
from ..exceptions import ConversionError
from ..translator import _
from ..transforms import get_import_context, get_export_context
from .base import BaseType

__all__ = ['UnionType']


def _valid_init_args(type_):
    args = set()
    for cls in type_.__mro__:
        try:
            init_args = inspect.getfullargspec(cls.__init__).args[1:]  # PY3
        except AttributeError:
            init_args = inspect.getargspec(cls.__init__).args[1:]  # PY2
        args.update(init_args)
        if cls is BaseType:
            break
    return args

def _filter_kwargs(valid_args, kwargs):
    return dict((k, v) for k, v in kwargs.items() if k in valid_args)


class UnionType(BaseType):

    types = None

    MESSAGES = {
        'convert': _("Couldn't interpret value '{0}' as any of {1}."),
    }

    _baseclass_args = _valid_init_args(BaseType)

    def __init__(self, types=None, resolver=None, **kwargs):

        self._types = OrderedDict()
        types = types or self.types
        if resolver:
            self.resolve = resolver

        for type_ in types:
            if isinstance(type_, type) and issubclass(type_, BaseType):
                type_ = type_(**_filter_kwargs(_valid_init_args(type_), kwargs))
            elif not isinstance(type_, BaseType):
                raise TypeError("Got '%s' instance instead of a Schematics type" % type_.__class__.__name__)
            self._types[type_.__class__] = type_
            self.typenames = tuple((cls.__name__ for cls in self._types))

        super(UnionType, self).__init__(**_filter_kwargs(self._baseclass_args, kwargs))

    def resolve(self, value, context):
        for field in self._types.values():
            try:
                value = field.convert(value, context)
            except ConversionError:
                pass
            else:
                return field, value
        return None

    def _resolve(self, value, context):
        response = self.resolve(value, context)
        if isinstance(response, type):
            field = self._types[response]
            try:
                response = field, field.convert(value, context)
            except ConversionError:
                pass
        if isinstance(response, tuple):
            return response
        raise ConversionError(self.messages['convert'].format(value, self.typenames))

    def convert(self, value, context=None):
        context = context or get_import_context()
        field, native_value = self._resolve(value, context)
        return native_value

    def validate(self, value, context=None):
        field, _ = self._resolve(value, context)
        return field.validate(value, context)

    def _export(self, value, format, context=None):
        field, _ = self._resolve(value, context)
        return field._export(value, format, context)

    def to_native(self, value, context=None):
        field, _ = self._resolve(value, context)
        return field.to_native(value, context)

    def to_primitive(self, value, context=None):
        field, _ = self._resolve(value, context)
        return field.to_primitive(value, context)


if PY2:
    # Python 2 names cannot be unicode
    __all__ = [n.encode('ascii') for n in __all__]
