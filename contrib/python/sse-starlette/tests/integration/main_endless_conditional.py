# main.py
import asyncio
import logging

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from sse_starlette import EventSourceResponse

"""
example by: justindujardin
4efaffc2365a85f132ab8fc405110120c9c9e36a, https://github.com/sysid/sse-starlette/pull/13

tests proper shutdown in case no messages are yielded:
- in a streaming endpoint that reports only on "new" data, it is possible to get into a state
    where no no yields are expected to happen in the near future.
    e.g. there are no new chat messages to emit.
- add a third task to taskgroup that checks the uvicorn exit status at a regular interval.
"""

_log = logging.getLogger(__name__)


async def endless(req: Request):
    """Simulates an endless stream but only yields one item

    In case of server shutdown the running task has to be stopped via signal handler in order
    to enable proper server shutdown. Otherwise, there will be dangling tasks preventing proper shutdown.
    (deadlock)
    """

    async def event_publisher():
        has_data = True  # The event publisher only conditionally emits items
        try:
            while True:
                disconnected = await req.is_disconnected()
                if disconnected:
                    _log.info(f"Disconnecting client {req.client}")
                    break
                # Simulate only sending one response
                if has_data:
                    yield dict(data="u can haz the data")
                    has_data = False
                await asyncio.sleep(0.9)
        except asyncio.CancelledError as e:
            _log.info(f"Disconnected from client (via refresh/close) {req.client}")
            # Do any other cleanup, if any
            raise e

    return EventSourceResponse(event_publisher())


async def healthcheck(req: Request):
    return JSONResponse({"status": "ok"})


app = Starlette(
    routes=[
        Route("/endless", endpoint=endless),
        Route("/health", endpoint=healthcheck),
    ],
)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="trace")
