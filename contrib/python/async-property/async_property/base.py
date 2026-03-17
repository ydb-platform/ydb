import asyncio
import functools

from async_property.proxy import AwaitableOnly

is_coroutine = asyncio.iscoroutinefunction


def async_property(func, *args, **kwargs):
    assert is_coroutine(func), 'Can only use with async def'
    return AsyncPropertyDescriptor(func, *args, **kwargs)


class AsyncPropertyDescriptor:
    def __init__(self, _fget, field_name=None):
        self._fget = _fget
        self.field_name = field_name or _fget.__name__
        functools.update_wrapper(self, _fget)

    def __set_name__(self, owner, name):
        self.field_name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return self.awaitable_only(instance)

    def __set__(self, instance, value):
        raise ValueError(INVALID_ACTION.format('set'))

    def __delete__(self, instance):
        raise ValueError(INVALID_ACTION.format('delete'))

    def get_loader(self, instance):
        @functools.wraps(self._fget)
        async def get_value():
            return await self._fget(instance)
        return get_value

    def awaitable_only(self, instance):
        return AwaitableOnly(self.get_loader(instance))


INVALID_ACTION = 'Cannot {} @async_property. ' \
                 'Use @async_cached_property instead.'
