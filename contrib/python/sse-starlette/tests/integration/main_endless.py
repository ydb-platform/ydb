# main.py
import asyncio
import logging

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from sse_starlette import EventSourceResponse

_log = logging.getLogger(__name__)


async def endless(req: Request):
    """Simulates an endless stream, events sent every 0.3 seconds"""

    async def event_publisher():
        i = 0
        try:
            while True:  # i <= 20:
                # yield dict(id=..., event=..., data=...)
                i += 1
                # print(f"Sending {i}")
                yield dict(data=i)
                await asyncio.sleep(0.3)
        except asyncio.CancelledError as e:
            _log.info(
                f"Disconnected from client (via refresh/close) {req.client} after {i} events"
            )
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
    import uvicorn

    uvicorn.run(app, host="localhost", port=8001, log_level="trace")
