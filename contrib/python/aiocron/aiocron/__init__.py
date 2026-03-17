# -*- coding: utf-8 -*-
from cronsim import CronSim
from datetime import datetime
from functools import wraps, partial
from tzlocal import get_localzone
from uuid import uuid4
import time
import asyncio
import sys
import inspect


async def null_callback(*args):
    return args


def wrap_func(func):
    """wrap in a coroutine"""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if inspect.isawaitable(result):
            result = await result
        return result

    return wrapper


class Cron(object):
    def __init__(
        self,
        spec,
        func=None,
        args=(),
        kwargs=None,
        start=False,
        uuid=None,
        loop=None,
        tz=None,
    ):
        self.spec = spec
        if func is not None:
            kwargs = kwargs or {}
            self.func = func if not (args or kwargs) else partial(func, *args, **kwargs)
        else:
            self.func = null_callback
        self.tz = get_localzone() if tz is None else tz
        self.cron = wrap_func(self.func)
        self.auto_start = start
        self.uuid = uuid if uuid is not None else uuid4()
        self.handle = self.future = self.cronsim = None
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        if start and self.func is not null_callback:
            self.handle = self.loop.call_soon_threadsafe(self.start)

    def start(self):
        """Start scheduling"""
        self.stop()
        self.initialize()
        self.handle = self.loop.call_at(self.get_next(), self.call_next)

    def stop(self):
        """Stop scheduling"""
        if self.handle is not None:
            self.handle.cancel()
        self.handle = self.future = self.cronsim = None

    async def next(self, *args):
        """yield from .next()"""
        self.initialize()
        self.future = asyncio.Future(loop=self.loop)
        self.handle = self.loop.call_at(self.get_next(), self.call_func, *args)
        return await self.future

    def initialize(self):
        """Initialize cronsim and related times"""
        if self.cronsim is None:
            self.time = time.time()
            self.datetime = datetime.now(self.tz)
            self.loop_time = self.loop.time()
            self.cronsim = CronSim(self.spec, self.datetime)

    def get_next(self):
        """Return next iteration time related to loop time"""
        return self.loop_time + (next(self.cronsim).timestamp() - self.time)

    def call_next(self):
        """Set next hop in the loop. Call task"""
        if self.handle is not None:
            self.handle.cancel()
        next_time = self.get_next()
        self.handle = self.loop.call_at(next_time, self.call_next)
        self.call_func()

    def call_func(self, *args, **kwargs):
        """Called. Take care of exceptions using gather"""
        """Check the version of python installed"""
        if sys.version_info[0:2] >= (3, 10):
            asyncio.gather(
                self.cron(*args, **kwargs), return_exceptions=True
            ).add_done_callback(self.set_result)
        else:
            asyncio.gather(
                self.cron(*args, **kwargs), loop=self.loop, return_exceptions=True
            ).add_done_callback(self.set_result)

    def set_result(self, result):
        """Set future's result if needed (can be an exception).
        Else raise if needed."""
        result = result.result()[0]
        if self.future is not None:
            if isinstance(result, Exception):
                self.future.set_exception(result)
            elif not self.future.done():
                self.future.set_result(result)
            self.future = None
        elif isinstance(result, Exception):
            raise result

    def __call__(self, func):
        """Used as a decorator"""
        self.func = func
        self.cron = wrap_func(func)
        if self.auto_start:
            self.loop.call_soon_threadsafe(self.start)
        return self

    def __str__(self):
        return "{0.spec} {0.func}".format(self)

    def __repr__(self):
        return "<Cron {0.spec} {0.func}>".format(self)


def crontab(spec, func=None, args=(), kwargs=None, start=True, loop=None, tz=None):
    return Cron(
        spec, func=func, args=args, kwargs=kwargs, start=start, loop=loop, tz=tz
    )
