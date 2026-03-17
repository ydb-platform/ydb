"""
Benchmark for asyncio websocket server and client performance
transferring 1MB of data.

Author: Kumar Aditya
"""

import pyperf
import websockets.server
import websockets.client
import websockets.exceptions
import asyncio

CHUNK_SIZE = 1024 ** 2
DATA = b"x" * CHUNK_SIZE

stop: asyncio.Event


async def handler(websocket) -> None:
    for _ in range(100):
        await websocket.recv()

    stop.set()


async def send(ws):
    try:
        await ws.send(DATA)
    except websockets.exceptions.ConnectionClosedOK:
        pass


async def main() -> None:
    global stop
    t0 = pyperf.perf_counter()
    stop = asyncio.Event()
    async with websockets.server.serve(handler, "", 8001):
        async with websockets.client.connect("ws://localhost:8001") as ws:
            await asyncio.gather(*[send(ws) for _ in range(100)])
        await stop.wait()
    return pyperf.perf_counter() - t0


if __name__ == "__main__":
    runner = pyperf.Runner()
    runner.metadata['description'] = "Benchmark asyncio websockets"
    runner.bench_async_func('asyncio_websockets', main)
