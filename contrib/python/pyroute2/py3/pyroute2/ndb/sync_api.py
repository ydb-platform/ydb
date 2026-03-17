import asyncio
import errno
from enum import IntFlag
from queue import Queue
from typing import ClassVar

from .report import RecordSet


class Flags(IntFlag):
    UNSPEC = 0
    RO = 1


class Permit:
    def __init__(self, *names: str):
        self.names = names

    def __iter__(self):
        return iter(self.names)


class SyncBase:

    permit_calls: ClassVar[Permit] = Permit('test')

    def __init__(self, event_loop, obj, class_map=None, flags=Flags.UNSPEC):
        self.event_loop = event_loop
        self.asyncore = obj
        self.flags = flags
        self.class_map = {} if class_map is None else class_map

    def check_permissions(self, name):
        if name in self.permit_calls:
            return
        if self.flags & Flags.RO:
            raise PermissionError('Access denied')

    def _get_sync_class(self, item, key=None):
        if key is None:
            key = self.asyncore.table
        return self.class_map.get(key, self.class_map.get('default'))(
            self.event_loop, item, self.class_map, self.flags
        )

    async def _tm_sync_generator(self, queue, func, *argv, **kwarg):
        for record in func(*argv, **kwarg):
            queue.put(record)
        queue.put(None)

    def _main_sync_generator(self, func, *argv, **kwarg):
        self.check_permissions(func.__name__)
        queue = Queue()
        task = asyncio.run_coroutine_threadsafe(
            self._tm_sync_generator(queue, func, *argv, **kwarg),
            self.event_loop,
        )
        while True:
            record = queue.get()
            if record is None:
                return
            yield record
        ret = task.result()
        if isinstance(ret, Exception):
            raise ret

    async def _tm_sync_call(self, func, *argv, **kwarg):
        return func(*argv, **kwarg)

    def _main_sync_call(self, func, *argv, **kwarg):
        self.check_permissions(func.__name__)
        task = asyncio.run_coroutine_threadsafe(
            self._tm_sync_call(func, *argv, **kwarg), self.event_loop
        )
        ret = task.result()
        if isinstance(ret, Exception):
            raise ret
        return ret

    def _main_async_call(self, func, *argv, **kwarg):
        self.check_permissions(func.__name__)
        task = asyncio.run_coroutine_threadsafe(
            func(*argv, **kwarg), self.event_loop
        )
        ret = task.result()
        if isinstance(ret, Exception):
            raise ret
        return ret

    def __eq__(self, other):
        return self.asyncore == getattr(other, 'asyncore', None)


class SyncDB(SyncBase):

    def export(self, f='stdout'):
        return self._main_sync_call(self.asyncore.schema.export)

    def backup(self, spec):
        return self._main_sync_call(self.asyncore.schema.backup, spec)

    def fetch(self, query):
        return self._main_sync_generator(self.asyncore.schema.fetch, query)

    def fetchone(self, query):
        return self._main_sync_call(self.asyncore.schema.fetchone, query)


class SyncView(SyncBase):

    permit_calls = Permit('summary', 'dump', '__getitem__')

    def __getitem__(self, key, table=None):
        item = self._main_sync_call(self.asyncore.__getitem__, key, table)
        return self._get_sync_class(item)

    def __contains__(self, key):
        return key in self.keys()

    def __iter__(self):
        return self.keys()

    def __len__(self):
        return self._main_sync_call(self.asyncore.__len__)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    @property
    def cache(self):
        return self.asyncore.cache

    def get(self, spec=None, table=None, **kwarg):
        item = self._main_sync_call(self.asyncore.get, spec, table, **kwarg)
        if item is not None:
            return self._get_sync_class(item)

    def getmany(self, spec, table=None):
        for item in tuple(
            self._main_sync_generator(self.asyncore.getmany, spec, table)
        ):
            yield item

    def getone(self, spec, table=None):
        return self._main_sync_call(self.asyncore.getone, spec, table)

    def items(self):
        for key in self.keys():
            yield (key, self[key])

    def keys(self):
        for record in self.dump():
            yield record

    def count(self):
        return self._main_sync_call(self.asyncore.count)

    def create(self, *argspec, **kwarg):
        item = self._main_sync_call(self.asyncore.create, *argspec, **kwarg)
        return self._get_sync_class(item)

    def ensure(self, *argspec, **kwarg):
        item = self._main_sync_call(self.asyncore.ensure, *argspec, **kwarg)
        return self._get_sync_class(item)

    def add(self, *argspec, **kwarg):
        item = self._main_sync_call(self.asyncore.add, *argspec, **kwarg)
        return self._get_sync_class(item)

    def wait(self, **spec):
        item = self._main_async_call(self.asyncore.wait, **spec)
        return self._get_sync_class(item)

    def exists(self, key, table=None):
        return self._main_sync_call(self.asyncore.exists, key, table)

    def locate(self, spec=None, table=None, **kwarg):
        item = self._main_sync_call(self.asyncore.locate, spec, table, **kwarg)
        return self._get_sync_class(item)

    def summary(self):
        return RecordSet(self._main_sync_generator(self.asyncore.summary))

    def dump(self):
        return RecordSet(self._main_sync_generator(self.asyncore.dump))


class SyncSources(SyncView):

    permit_calls = Permit('add', 'remove', 'keys', '__getitem__')

    def __getitem__(self, key):
        item = self._main_sync_call(self.asyncore.__getitem__, key)
        return self._get_sync_class(item)

    def add(self, **spec):
        item = self._main_async_call(self.asyncore.add, **spec)
        return self._get_sync_class(item)

    def remove(self, target, code=errno.ECONNRESET, sync=True):
        item = self._main_sync_call(self.asyncore.remove, target, code, sync)
        return self._get_sync_class(item)

    def keys(self):
        for record in self.asyncore.keys():
            yield record
