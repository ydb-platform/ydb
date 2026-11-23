"""Forward systemd socket connections to a reachable local YDB monitoring port."""

import asyncio
import socket
import sys


async def copy_stream(reader, writer):
    while True:
        chunk = await reader.read(65536)
        if not chunk:
            writer.write_eof()
            return
        writer.write(chunk)
        await writer.drain()


async def forward(client, downstream, ports):
    upstream = None
    pumps = []
    try:
        for port in ports:
            try:
                server, upstream = await asyncio.wait_for(asyncio.open_connection("::1", port), timeout=1)
                break
            except (OSError, asyncio.TimeoutError):
                continue
        if upstream:
            pumps = [
                asyncio.create_task(copy_stream(client, upstream)),
                asyncio.create_task(copy_stream(server, downstream)),
            ]
            await asyncio.gather(*pumps)
    except (OSError, ConnectionError):
        pass
    finally:
        for pump in pumps:
            pump.cancel()
        downstream.close()
        if upstream:
            upstream.close()


async def main(ports):
    listener = socket.socket(fileno=3)
    server = await asyncio.start_server(lambda r, w: forward(r, w, ports), sock=listener)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main([int(arg) for arg in sys.argv[1:]]))
