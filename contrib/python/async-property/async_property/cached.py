import asyncio
import functools
from collections import defaultdict

from async_property.proxy import AwaitableOnly, AwaitableProxy

is_coroutine = asyncio.iscoroutinefunction


ASYNC_PROPERTY_ATTR = '__async_property__'


def async_cached_property(func, *args, **kwargs):
    assert is_coroutine(func), 'Can only use with async def'
    return AsyncCachedPropertyDescriptor(func, *args, **kwargs)


class AsyncCachedPropertyInstanceState:
    def __init__(self):
        self.cache = {}
        self.lock = defaultdict(asyncio.Lock)

    __slots__ = 'cache', 'lock'


class AsyncCachedPropertyDescriptor:
    def __init__(self, _fget, _fset=None, _fdel=None, field_name=None):
        self._fget = _fget
        self._fset = _fset
        self._fdel = _fdel
        self.field_name = field_name or _fget.__name__

        functools.update_wrapper(self, _fget)
        self._check_method_sync(_fset, 'setter')
        self._check_method_sync(_fdel, 'deleter')

    def __set_name__(self, owner, name):
        self.field_name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        if self.has_cache_value(instance):
            return self.already_loaded(instance)
        return self.not_loaded(instance)

    def __set__(self, instance, value):
        if self._fset is not None:
            self._fset(instance, value)
        self.set_cache_value(instance, value)

    def __delete__(self, instance):
        if self._fdel is not None:
            self._fdel(instance)
        self.del_cache_value(instance)

    def setter(self, method):
        self._check_method_name(method, 'setter')
        return type(self)(self._fget, method, self._fdel, self.field_name)

    def deleter(self, method):
        self._check_method_name(method, 'deleter')
        return type(self)(self._fget, self._fset, method, self.field_name)

    def _check_method_name(self, method, method_type):
        if method.__name__ != self.field_name:
            raise AssertionError(
                f'@{self.field_name}.{method_type} name must match property name'
            )

    def _check_method_sync(self, method, method_type):
        if method and is_coroutine(method):
            raise AssertionError(
                f'@{self.field_name}.{method_type} must be synchronous'
            )

    def get_instance_state(self, instance):
        try:
            return getattr(instance, ASYNC_PROPERTY_ATTR)
        except AttributeError:
            state = AsyncCachedPropertyInstanceState()
            object.__setattr__(instance, ASYNC_PROPERTY_ATTR, state)
            return state

    def get_lock(self, instance):
        lock = self.get_instance_state(instance).lock
        return lock[self.field_name]

    def get_cache(self, instance):
        return self.get_instance_state(instance).cache

    def has_cache_value(self, instance):
        cache = self.get_cache(instance)
        return self.field_name in cache

    def get_cache_value(self, instance):
        cache = self.get_cache(instance)
        return cache[self.field_name]

    def set_cache_value(self, instance, value):
        cache = self.get_cache(instance)
        cache[self.field_name] = value

    def del_cache_value(self, instance):
        cache = self.get_cache(instance)
        del cache[self.field_name]

    def get_loader(self, instance):
        @functools.wraps(self._fget)
        async def load_value():
            async with self.get_lock(instance):
                if self.has_cache_value(instance):
                    return self.get_cache_value(instance)
                value = await self._fget(instance)
                self.__set__(instance, value)
                return value
        return load_value

    def already_loaded(self, instance):
        return AwaitableProxy(self.get_cache_value(instance))

    def not_loaded(self, instance):
        return AwaitableOnly(self.get_loader(instance))
