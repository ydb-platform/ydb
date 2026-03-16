# Third-party imports
import asyncio
import sys

# Local models
from uplink.clients.io import interfaces

__all__ = ["AsyncioStrategy"]


class AsyncioStrategy(interfaces.IOStrategy):
    """A non-blocking execution strategy using asyncio."""

    async def invoke(self, func, args, kwargs, callback):
        try:
            response = await func(*args, **kwargs)
        except Exception as error:
            tb = sys.exc_info()[2]
            response = await callback.on_failure(type(error), error, tb)
        else:
            response = await callback.on_success(response)
        return response

    async def sleep(self, duration, callback):
        await asyncio.sleep(duration)
        return await callback.on_success()

    async def finish(self, response):
        return response

    async def execute(self, executable):
        return await executable.execute()
