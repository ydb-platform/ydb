"""
This module defines common converter keys, used by consumers of the
converter layer to identify the desired conversion type when querying a
:py:class:`uplink.converters.ConverterFactoryRegistry`.
"""
# Standard library imports
import functools

# Local imports

__all__ = [
    "CONVERT_TO_STRING",
    "CONVERT_TO_REQUEST_BODY",
    "CONVERT_FROM_RESPONSE_BODY",
    "Map",
    "Sequence",
]

#: Object to string conversion.
CONVERT_TO_STRING = 0

#: Object to request body conversion.
CONVERT_TO_REQUEST_BODY = 1

# Response body to object conversion.
CONVERT_FROM_RESPONSE_BODY = 2


class CompositeKey(object):
    """
    A utility class for defining composable converter keys.

    Arguments:
        converter_key: The enveloped converter key.
    """

    def __init__(self, converter_key):
        self._converter_key = converter_key

    def __eq__(self, other):
        if isinstance(other, CompositeKey) and type(other) is type(self):
            return other._converter_key == self._converter_key
        return False

    def convert(self, converter, value):  # pragma: no cover
        raise NotImplementedError

    def __call__(self, converter_registry):
        factory = converter_registry[self._converter_key]

        def factory_wrapper(*args, **kwargs):
            converter = factory(*args, **kwargs)
            if not converter:
                return None
            return functools.partial(self.convert, converter)

        return factory_wrapper


class Map(CompositeKey):
    """
    Object to mapping conversion.

    The constructor argument :py:data:`converter_key` details the type
    for the values in the mapping. For instance::

        # Key for conversion of an object to a mapping of strings.
        Map(CONVERT_TO_STRING)
    """

    def convert(self, converter, value):
        return dict((k, converter(value[k])) for k in value)


class Sequence(CompositeKey):
    """
    Object to sequence conversion.

    The constructor argument :py:data:`converter_key` details the type
    for the elements in the sequence. For instance::

        # Key for conversion of an object to a sequence of strings.
        Sequence(CONVERT_TO_STRING)
    """

    def convert(self, converter, value):
        if isinstance(value, (list, tuple)):
            return list(map(converter, value))
        else:
            return converter(value)


class Identity(object):
    """
    Identity conversion - pass value as is
    """

    def __call__(self, converter_registry):
        return self._identity_factory

    def __eq__(self, other):
        return type(other) is type(self)

    def _identity_factory(self, *args, **kwargs):
        return self._identity

    def _identity(self, value):
        return value
