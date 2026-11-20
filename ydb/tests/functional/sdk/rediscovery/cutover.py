"""Exercise a persistent SDK driver against two disposable, plaintext YDB databases."""

import argparse
import asyncio
import os
from contextlib import AsyncExitStack, suppress
from pathlib import Path

import grpc
from ydb._grpc.v6 import ydb_discovery_v1_pb2_grpc as discovery
from ydb._grpc.v6.protos import ydb_discovery_pb2 as proto
from ydb._grpc.v6.protos import ydb_status_codes_pb2 as status


CLIENTS = {
    "cpp": ["./rediscovery-probe"],
    "dotnet": [os.environ.get("DOTNET", "dotnet"), "bin/Release/net8.0/RediscoveryProbe.dll"],
    "go": ["./rediscovery-probe"],
    "java": [os.environ.get("JAVA", "java"), "-cp", "target/classes:target/dependency/*", "RediscoveryProbe"],
    "javascript": [os.environ.get("NODE", "node"), "main.mjs"],
    "php": [os.environ.get("PHP", "php"), "main.php"],
    "python": [os.environ.get("PYTHON", "python3"), "main.py"],
    "rust": ["./target/debug/ydb-rediscovery-probe"],
}


class Relay:
    """Observe client HTTP/2 request headers and sever a real TCP transport mid-call."""

    def __init__(self, upstream, route):
        self.host, port = upstream.rsplit(":", 1)
        self.port = int(port)
        self.route = route
        self.sockets = set()

    async def start(self, stack):
        self.listener = await stack.enter_async_context(
            await asyncio.start_server(self.forward, "127.0.0.1", 0)
        )
        stack.callback(self.close)
        self.address = self.listener.sockets[0].getsockname()[1]

    def close(self):
        self.listener.close()
        for writer in self.sockets:
            writer.transport.abort()

    async def forward(self, client, writer):
        self.sockets.add(writer)
        self.route.count_connections()
        try:
            upstream, backend = await asyncio.open_connection(self.host, self.port)
            try:
                async def requests():
                    # The HTTP/2 connection preface is followed by length-prefixed frames.
                    backend.write(await client.readexactly(24))
                    while True:
                        header = await client.readexactly(9)
                        length = int.from_bytes(header[:3], "big")
                        if header[3] == 1:  # HEADERS starts an RPC on this stream
                            self.route.request(self)
                        backend.write(header + await client.readexactly(length))
                        await backend.drain()

                async def responses():
                    while data := await upstream.read(65536):
                        writer.write(data)
                        await writer.drain()

                tasks = [asyncio.create_task(coro()) for coro in (requests, responses)]
                try:
                    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                backend.close()
        except (OSError, asyncio.IncompleteReadError):
            pass
        finally:
            self.sockets.discard(writer)
            writer.close()


class Route(discovery.DiscoveryServiceServicer):
    def __init__(self, args):
        self.database = args.database
        self.sources = [Relay(args.source, self) for _ in range(2)]
        self.target = Relay(args.destination, self)
        self.bootstrap = None
        self.last_source = None
        self.armed = False
        self.cut = asyncio.Event()
        self.discovered = False
        self.reached = False
        self.stale = False
        self.before_peak = self.after_peak = 0

    def count_connections(self):
        count = sum(len(relay.sockets) for relay in [*self.sources, self.target, self.bootstrap])
        if self.cut.is_set():
            self.after_peak = max(self.after_peak, count)
        else:
            self.before_peak = max(self.before_peak, count)

    def switch(self, source):
        self.cut.set()
        self.count_connections()
        source.close()  # Keep the other advertised source address healthy.

    def request(self, relay):
        if relay is self.bootstrap:
            return
        if self.cut.is_set():
            if relay is self.target:
                self.reached = True
            else:
                self.stale = True
        elif self.armed:
            self.switch(relay)
            raise ConnectionResetError("injected source loss")
        else:
            self.last_source = relay

    async def ListEndpoints(self, request, context):
        if request.database != self.database:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "unexpected database")
        endpoints = self.sources
        if self.cut.is_set():
            endpoints = [self.target]
            self.discovered = True
        result = proto.ListEndpointsResult()
        for relay in endpoints:
            result.endpoints.add(address="127.0.0.1", port=relay.address, node_id=relay.node_id)
        response = proto.ListEndpointsResponse()
        response.operation.ready = True
        response.operation.status = status.StatusIds.SUCCESS
        response.operation.result.Pack(result)
        return response


async def run(args):
    modes = {
        "cpp": {"scheme", "bulk-upsert", "topic-read", "topic-write"},
        "python": {"scheme", "topic-read", "topic-write"},
        "go": {"scheme", "topic-write"},
    }
    if args.probe not in modes.get(args.client, {"scheme"}):
        raise ValueError(f"{args.client} probe does not implement {args.probe}")
    if args.source == args.destination:
        raise ValueError("provide distinct source and destination backends")
    route = Route(args)
    async with AsyncExitStack() as stack:
        for relay in [*route.sources, route.target]:
            await relay.start(stack)
        # Use the real ID for direct Topic partition lookups. The extra source
        # address models a still-reachable peer in a cached discovery response.
        for endpoint, relays in ((args.source, route.sources), (args.destination, [route.target])):
            async with grpc.aio.insecure_channel(endpoint) as channel:
                reply = await discovery.DiscoveryServiceStub(channel).ListEndpoints(
                    proto.ListEndpointsRequest(database=args.database), timeout=5
                )
                topology = proto.ListEndpointsResult()
                if not reply.operation.result.Unpack(topology) or not topology.endpoints:
                    raise RuntimeError(f"backend discovery failed: {reply.operation}")
                for index, relay in enumerate(relays):
                    relay.node_id = topology.endpoints[0].node_id + index

        server = grpc.aio.server()
        discovery.add_DiscoveryServiceServicer_to_server(route, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        stack.push_async_callback(server.stop, 0)
        route.bootstrap = Relay(f"127.0.0.1:{port}", route)
        await route.bootstrap.start(stack)
        client = await asyncio.create_subprocess_exec(
            *CLIENTS[args.client],
            cwd=Path(__file__).resolve().parent / "clients" / args.client,
            env={**os.environ, "YDB_ENDPOINT": f"127.0.0.1:{route.bootstrap.address}",
                 "YDB_DATABASE": args.database, "YDB_PROBE": args.probe},
            stdout=asyncio.subprocess.PIPE,
        )
        async def success(endpoint):
            expected = f"OK {endpoint}".encode() if args.probe == "topic-read" else b"OK"
            while line := await client.stdout.readline():
                if line.strip() == expected:
                    return
            raise RuntimeError(f"probe exited with status {await client.wait()}")

        try:
            await asyncio.wait_for(success(args.source), timeout=40)
            if route.last_source is None:
                raise AssertionError("warm operation bypassed the discovered endpoints")
            if args.probe.startswith("topic-"):
                route.switch(route.last_source)
            else:
                route.armed = True  # Cut the next unary RPC while it is in flight.
            await asyncio.wait_for(route.cut.wait(), timeout=10)
            async with asyncio.timeout(args.timeout):
                while True:
                    await success(args.destination)
                    if route.stale:
                        raise AssertionError("SDK sent a new RPC to a cached source endpoint")
                    if route.discovered and route.reached:
                        break
            if route.after_peak > route.before_peak:
                raise AssertionError(f"peak connections increased: {route.before_peak} -> {route.after_peak}")
        finally:
            if client.returncode is None:
                with suppress(ProcessLookupError):
                    client.kill()
            await client.wait()
            print(f"{args.client} {args.probe}: rediscovered={route.discovered}, "
                  f"destination_rpc={route.reached}, stale_rpc={route.stale}, "
                  f"peak_connections={route.before_peak}/{route.after_peak}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True, choices=sorted(CLIENTS))
    parser.add_argument("--source", required=True, help="disposable source ydbd host:port")
    parser.add_argument("--destination", required=True, help="disposable destination ydbd host:port")
    parser.add_argument("--database", default="/local")
    parser.add_argument("--probe", choices=["scheme", "bulk-upsert", "topic-read", "topic-write"], default="scheme")
    parser.add_argument("--timeout", type=float, default=4, help="maximum seconds after transport loss")
    asyncio.run(run(parser.parse_args()))
