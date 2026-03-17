# Standard library imports
import collections
import functools

# Local imports
from uplink.compat import abc
from uplink.converters import interfaces, register_default_converter_factory

__all__ = ["TypingConverter", "ListConverter", "DictConverter"]


class BaseTypeConverter(object):
    Builder = collections.namedtuple("Builder", "build")

    @classmethod
    def freeze(cls, *args, **kwargs):
        return cls.Builder(functools.partial(cls, *args, **kwargs))


class ListConverter(BaseTypeConverter, interfaces.Converter):
    def __init__(self, elem_type):
        self._elem_type = elem_type
        self._elem_converter = None

    def set_chain(self, chain):
        self._elem_converter = chain(self._elem_type) or self._elem_type

    def convert(self, value):
        if isinstance(value, abc.Sequence):
            return list(map(self._elem_converter, value))
        else:
            # TODO: Handle the case where the value is not an sequence.
            return [self._elem_converter(value)]


class DictConverter(BaseTypeConverter, interfaces.Converter):
    def __init__(self, key_type, value_type):
        self._key_type = key_type
        self._value_type = value_type
        self._key_converter = None
        self._value_converter = None

    def set_chain(self, chain):
        self._key_converter = chain(self._key_type) or self._key_type
        self._value_converter = chain(self._value_type) or self._value_type

    def convert(self, value):
        if isinstance(value, abc.Mapping):
            key_c, val_c = self._key_converter, self._value_converter
            return dict((key_c(k), val_c(value[k])) for k in value)
        else:
            # TODO: Handle the case where the value is not a mapping.
            return self._value_converter(value)


class _TypeProxy(object):
    def __init__(self, func):
        self._func = func

    def __getitem__(self, item):
        items = item if isinstance(item, tuple) else (item,)
        return self._func(*items)


def _get_types(try_typing=True):
    if TypingConverter.typing and try_typing:
        return TypingConverter.typing.List, TypingConverter.typing.Dict
    else:
        return (
            _TypeProxy(ListConverter.freeze),
            _TypeProxy(DictConverter.freeze),
        )


@register_default_converter_factory
class TypingConverter(interfaces.Factory):
    """
    .. versionadded: v0.5.0

    An adapter that serializes and deserializes collection types from
    the :py:mod:`typing` module, such as :py:class:`typing.List`.

    Inner types of a collection are recursively resolved, using other
    available converters if necessary. For instance, when resolving the
    type hint :py:attr:`typing.Sequence[UserSchema]`, where
    :py:attr:`UserSchema` is a custom :py:class:`marshmallow.Schema`
    subclass, the converter will resolve the inner type using
    :py:class:`uplink.converters.MarshmallowConverter`.

    .. code-block:: python

        @get("/users")
        def get_users(self) -> typing.Sequence[UserSchema]:
            '''Fetch all users.'''

    Note:

        The :py:mod:`typing` module is available in the standard library
        starting from Python 3.5. For earlier versions of Python, there
        is a port of the module available on PyPI.

        However, you can utilize this converter without the
        :py:mod:`typing` module by using one of the proxies defined by
        :py:class:`uplink.returns` (e.g., :py:obj:`uplink.types.List`).
    """

    try:
        import typing
    except ImportError:  # pragma: no cover
        typing = None

    def _check_typing(self, t):
        has_origin = hasattr(t, "__origin__")
        has_args = hasattr(t, "__args__")
        return self.typing and has_origin and has_args

    def _base_converter(self, type_):
        if isinstance(type_, BaseTypeConverter.Builder):
            return type_.build()
        elif self._check_typing(type_):
            if issubclass(type_.__origin__, self.typing.Sequence):
                return ListConverter(*type_.__args__)
            elif issubclass(type_.__origin__, self.typing.Mapping):
                return DictConverter(*type_.__args__)

    def create_response_body_converter(self, type_, *args, **kwargs):
        return self._base_converter(type_)

    def create_request_body_converter(self, type_, *args, **kwargs):
        return self._base_converter(type_)


TypingConverter.List, TypingConverter.Dict = _get_types()
