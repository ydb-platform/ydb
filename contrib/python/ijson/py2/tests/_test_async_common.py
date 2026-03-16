# -*- coding:utf-8 -*-

import asyncio
import contextlib


def _aiorun(f):
    with contextlib.closing(asyncio.new_event_loop()) as loop:
        loop.run_until_complete(f)


def _get_all(reader):
    def get_all(routine, json_content, *args, **kwargs):
        events = []
        async def run():
            async for event in routine(reader(json_content), *args, **kwargs):
                events.append(event)
        _aiorun(run())
        return events
    return get_all


def _get_first(reader):
    def get_first(routine, json_content, *args, **kwargs):
        events = []
        async def run():
            async for event in routine(reader(json_content), *args, **kwargs):
                events.append(event)
                if events:
                    return
        _aiorun(run())
        return events[0]
    return get_first
