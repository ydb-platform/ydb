import asyncio

import ijson

from ._async_common import _aiorun


async def _async_chunks(json, chunk_size=1):
    for i in range(0, len(json), chunk_size):
        await asyncio.sleep(0)
        yield json[i : i + chunk_size]


def get_all(routine, json_content, *args, **kwargs):
    events = []

    async def run():
        reader = ijson.from_iter(_async_chunks(json_content))
        async for event in routine(reader, *args, **kwargs):
            events.append(event)

    _aiorun(run())
    return events
