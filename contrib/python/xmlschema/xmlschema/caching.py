#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from threading import Lock
from collections.abc import Callable
from functools import lru_cache, wraps
from typing import Any, Generic, overload, TypeVar, TYPE_CHECKING, Union, cast

from xmlschema.aliases import SchemaType
from xmlschema.exceptions import XMLSchemaAttributeError, XMLSchemaTypeError, XMLSchemaValueError

if TYPE_CHECKING:
    from xmlschema.validators.xsdbase import XsdComponent  # noqa


T = TypeVar('T', bound=Union[SchemaType, 'XsdComponent'])
RT = TypeVar('RT', covariant=True)


_cached_functions: dict[Callable[..., Any], tuple[int | None, bool]] = {}


class SchemaCache:

    __slots__ = ('_enabled', '_functions', '_caches', '_lock')

    def __init__(self, enabled: bool = True) -> None:
        self._enabled = enabled
        self._functions = _cached_functions.copy()
        self._caches: dict[Callable[..., Any], Any] = {}
        self._lock = Lock()
        self._create_caches()

    def __call__(self, func: Callable[..., RT], *args: Any, **kwargs: Any) -> RT:
        try:
            return cast(RT, self._caches[func](*args, **kwargs))
        except KeyError:
            with self._lock:
                return cast(RT, self._caches[func](*args, **kwargs))

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool) -> None:
        if value is not self._enabled:
            self._enabled = value
            with self._lock:
                self._create_caches()

    def _create_caches(self) -> None:
        if self._enabled:
            for func, (maxsize, typed) in self._functions.items():
                self._caches[func] = lru_cache(maxsize, typed)(func)
        else:
            # Caching is not enables: fallback to registered functions
            for func in self._functions:
                self._caches[func] = func

    def register(self, func: Callable[..., RT],
                 maxsize: int | None = None,
                 typed: bool = True) -> None:
        if not callable(func):
            raise XMLSchemaTypeError(f"{func!r} is not callable")

        with self._lock:
            if func in self._functions:
                raise XMLSchemaValueError(f"{func!r} is already cached by {self!r}")
            self._functions[func] = maxsize, typed
            self._caches[func] = lru_cache(maxsize, typed)(func) if self._enabled else func

    def clear(self) -> None:
        with self._lock:
            if self._enabled:
                for cache in self._caches.values():
                    cache.cache_clear()


def schema_lru_cache(maxsize: int | None = None, typed: bool = False) -> Callable[..., RT]:
    """
    Cache an XSD validator method using an LRU cache stored in XSD global maps cache.
    """
    def wrapper_cached(func: Callable[..., RT]) -> Callable[..., RT]:
        _cached_functions[func] = maxsize, typed

        @wraps(func)
        def wrapped_func(validator: T, *args: Any, **kwargs: Any) -> RT:
            return validator.maps.cache(func, validator, *args, **kwargs)
        return wrapped_func

    return cast(Callable[..., RT], wrapper_cached)


def schema_cache(func: Callable[..., RT]) -> Callable[..., RT]:
    """
    Cache an XSD validator method using an unlimited cache stored in XSD global maps cache.
    """
    _cached_functions[func] = None, False

    @wraps(func)
    def wrapped_func(validator: T, *args: Any, **kwargs: Any) -> RT:
        return validator.maps.cache(func, validator, *args, **kwargs)
    return wrapped_func


# noinspection PyPep8Naming
class schema_cached_property(Generic[T, RT]):
    """
    A property that caches the value on the :class:`SchemaCache` of the global maps.
    """
    __slots__ = ('func', '_name', '__dict__')

    def __init__(self, func: Callable[[T], RT]) -> None:
        _cached_functions[func] = None, False
        self.func = func
        self.__doc__ = func.__doc__
        if hasattr(func, '__module__'):
            self.__module__ = func.__module__

    def __set_name__(self, owner: type[T], name: str) -> None:
        if not hasattr(owner, 'built'):
            raise XMLSchemaTypeError("{!r} is not an XSD validator".format(owner))
        if name == 'built':
            raise XMLSchemaAttributeError("can't apply to 'built' property")
        self._name = name

    @overload
    def __get__(self, validator: None, owner: type[T]) -> Callable[[T], RT]:
        ...

    @overload
    def __get__(self, validator: T, owner: type[T]) -> RT:
        ...

    def __get__(self, validator: T | None, owner: type[T]) -> Callable[[T], RT] | RT:
        if validator is None:
            return self.func
        elif not validator.maps.built:
            return self.func(validator)
        return validator.maps.cache(self.func, validator)
