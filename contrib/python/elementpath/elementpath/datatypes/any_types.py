#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from abc import ABCMeta, abstractmethod
from types import MappingProxyType
from typing import Any

from elementpath.helpers import LazyPattern
from elementpath.namespaces import XSD_NAMESPACE

###
# Classes for XSD built-in atomic types. All defined classes use a
# metaclass that adds some common methods and registers each class
# into a dictionary. Some classes of XSD primitive types are defined
# as proxies of basic Python datatypes.

_builtin_atomic_types: dict[str, 'AtomicTypeMeta'] = {}
builtin_atomic_types = MappingProxyType(_builtin_atomic_types)
"""Registry of builtin atomic types by expanded name and prefixed name."""


class AtomicTypeMeta(ABCMeta):
    """
    Metaclass for creating builtin atomic types. The created classes
    are decorated with missing attributes and methods. When a name
    attribute is provided the class is registered into two maps of
    atomic referenced by expanded name and prefixed name. For default
    the names are mapped to XSD namespace with 'xs' as the prefix.
    """
    types_map = _builtin_atomic_types

    def __new__(mcs, class_name: str, bases: tuple[type[Any], ...],
                dict_: dict[str, Any]) -> 'AtomicTypeMeta':
        try:
            name = dict_['name']
        except KeyError:
            name = dict_['name'] = None  # do not inherit name

        if name is not None and not isinstance(name, str):
            raise TypeError("attribute 'name' must be a string or None")

        if '__slots__' not in dict_:
            dict_['__slots__'] = ()  # Avoids __dict__ creation

        cls = super(AtomicTypeMeta, mcs).__new__(mcs, class_name, bases, dict_)

        # Register all the derived classes with a valid name if not already registered
        if name:
            namespace: str | None = dict_.pop('namespace', XSD_NAMESPACE)
            prefix: str | None = dict_.pop('prefix', 'xs')

            extended_name = f'{{{namespace}}}{name}' if namespace else name
            prefixed_name = f'{prefix}:{name}' if prefix else name

            if not cls.__doc__:
                if ver := getattr(cls, '_xsd_version', ''):
                    ver = ver + ' '
                params = getattr(cls, '__init__', None).__doc__ or ''
                cls.__doc__ = f"Class for XSD{ver} {prefixed_name} builtin datatype.\n{params}"

            if extended_name not in cls.types_map:
                assert prefixed_name not in mcs.types_map, \
                    'prefix already used by another namespace'
                mcs.types_map[extended_name] = mcs.types_map[prefixed_name] = cls

        return cls


class AnyType(metaclass=ABCMeta):
    """
    xs:anyType: the root type for all XML Schema types. Not definable as a concrete type by XPath.
    """

    name: str | None = None          # Register the class if a name is provided.
    namespace: str | None = None     # Namespace URI, defaults to XSD namespace.
    prefix: str | None = None        # Namespace prefix: defaults to 'xs' prefix.
    _xsd_version: str | None = None  # Overridden by types compatible with either XSD 1.0 or 1.1

    @abstractmethod
    def __init__(self, obj: Any) -> None:
        raise NotImplementedError()

    @classmethod
    def make(cls, value: Any, *args: Any, **kwargs: Any) -> 'AnyType | float':
        """
        Versioned factory method to create XSD type instances.

        :param value: the value to be converted to the XSD type.
        :param kwargs: additional keyword arguments to be passed to factory.
        """
        return cls(value)

    @classmethod
    def validate(cls, value: Any) -> None:
        """Validate the provided value against the XSD type."""
        if not isinstance(value, object):
            raise cls._invalid_type(value)  # noqa

    @classmethod
    def is_valid(cls, value: Any) -> bool:
        """Return whether the provided value is a valid XSD type."""
        try:
            cls.validate(value)
        except (TypeError, ValueError):
            return False
        else:
            return True

    @classmethod
    def _invalid_type(cls, obj: object) -> TypeError:
        if cls.name:
            return TypeError('invalid type {!r} for xs:{}'.format(type(obj), cls.name))
        return TypeError('invalid type {!r} for {!r}'.format(type(obj), cls))

    @classmethod
    def _invalid_value(cls, obj: object) -> ValueError:
        if cls.name:
            return ValueError('invalid value {!r} for xs:{}'.format(obj, cls.name))
        return ValueError('invalid value {!r} for {!r}'.format(obj, cls))

    @property
    def xpath_versions(self) -> tuple[str, ...] | str:
        """Return the XPath versions that the instance is compatible with."""
        return '2.0+'

    @property
    def xsd_versions(self) -> tuple[str, str] | str:
        """Return the XSD versions that the instance is compatible with."""
        return self._xsd_version or ('1.0', '1.1')


class AnySimpleType(AnyType):
    """
    xs:anySimpleType: the base type of all XSD simple types (atomic types,
    union types and list types). Not applicable as concrete type by XPath.
    """


class AnyAtomicType(AnySimpleType, metaclass=AtomicTypeMeta):
    """xs:anyAtomicType: the base type of all XSD atomic types."""
    name = 'anyAtomicType'
    pattern = LazyPattern(r'^$')

    @abstractmethod
    def __init__(self, value: Any) -> None:
        raise NotImplementedError()

    @classmethod
    def validate(cls, value: object) -> None:
        if isinstance(value, cls):
            return
        elif isinstance(value, str):
            if cls.pattern.match(value) is None:
                raise cls._invalid_value(value)
        else:
            raise cls._invalid_type(value)
