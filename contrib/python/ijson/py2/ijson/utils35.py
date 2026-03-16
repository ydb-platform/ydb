'''
Python3.5+ specific utilities
'''
import collections

from ijson import utils, common, compat


class utf8reader_async(compat.utf8reader):
    """
    Takes a utf8-encoded string asynchronous reader and asynchronously reads
    bytes out of it
    """
    async def read(self, n):
        data = await self.str_reader.read(n)
        return data.encode('utf-8')

async def _get_read(f):
    """Returns an awaitable read function that reads the requested type"""
    if type(await f.read(0)) == compat.bytetype:
        return f.read
    return compat._warn_and_return(utf8reader_async(f).read)

class sendable_deque(collections.deque):
    '''Like utils.sendable_list, but for deque objects'''
    send = collections.deque.append

class async_iterable(object):
    '''
    A utility class that implements an async iterator returning values
    dispatched by a coroutine pipeline after *it* has received values coming
    from an async file-like object.
    '''

    def __init__(self, f, buf_size, *coro_pipeline):
        self.events = sendable_deque()
        self.coro = utils.chain(self.events, *coro_pipeline)
        self.coro_finished = False
        self.f = f
        self.buf_size = buf_size
        self.read = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.read:
            self.read = await _get_read(self.f)
        if self.events:
            return self.events.popleft()
        if self.coro_finished:
            raise StopAsyncIteration
        while True:
            data = await self.read(self.buf_size)
            try:
                self.coro.send(data)
                if self.events:
                    return self.events.popleft()
            except StopIteration:
                self.coro_finished = True
                if self.events:
                    return self.events.popleft()
                raise StopAsyncIteration


def _make_basic_parse_async(backend):
    def basic_parse_async(f, buf_size=64*1024, **config):
        return async_iterable(f, buf_size,
            *common._basic_parse_pipeline(backend, config)
        )
    return basic_parse_async

def _make_parse_async(backend):
    def parse_async(f, buf_size=64*1024, **config):
        return async_iterable(f, buf_size,
            *common._parse_pipeline(backend, config)
        )
    return parse_async

def _make_items_async(backend):
    def items_async(f, prefix, map_type=None, buf_size=64*1024, **config):
        return async_iterable(f, buf_size,
            *common._items_pipeline(backend, prefix, map_type, config)
        )
    return items_async

def _make_kvitems_async(backend):
    def kvitems_async(f, prefix, map_type=None, buf_size=64*1024, **config):
        return async_iterable(f, buf_size,
            *common._kvitems_pipeline(backend, prefix, map_type, config)
        )
    return kvitems_async