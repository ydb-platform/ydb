"""Exercise a persistent SDK driver against two disposable, plaintext YDB databases."""

import argparse
import asyncio
import http.client
import json
import os
import re
from contextlib import AsyncExitStack, suppress
from pathlib import Path
from urllib.parse import urlencode, urlsplit

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


def disconnect(monitor, database, token):
    url = urlsplit(monitor)
    if url.scheme not in {"http", "https"} or not url.hostname or url.username is not None or url.query or url.fragment:
        raise ValueError("expected a node monitoring URL")
    protocol = http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
    path = url.path.rstrip("/") + "/actors/grpc_disconnect?" + urlencode({"database": database})
    # Do not forward the Authorization header to a redirect target.
    connection = protocol(url.hostname, url.port, timeout=5)
    try:
        connection.request("POST", path, body=b"", headers={"Authorization": token})
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError(f"disconnect returned HTTP {response.status} on {url.netloc}")
        if json.load(response) != {"status": "OK"}:
            raise RuntimeError(f"unexpected disconnect response on {url.netloc}")
    finally:
        connection.close()


class Relay:
    """Observe client HTTP/2 request headers and sever a real TCP transport mid-call."""

    def __init__(self, upstream, route):
        self.endpoint = upstream
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
                    if self.route.node_disconnect and self in self.route.sources:
                        while True:
                            try:
                                header = await upstream.readexactly(9)
                                data = await upstream.readexactly(int.from_bytes(header[:3], "big"))
                            except (OSError, asyncio.IncompleteReadError):
                                self.route.backend_loss("socket closed")
                                return
                            writer.write(header + data)
                            if header[3] == 7:  # HTTP/2 GOAWAY
                                self.route.backend_loss("GOAWAY")
                            await writer.drain()
                    else:
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
        self.node_disconnect = bool(args.source_admin)
        self.sources = [Relay(endpoint, self) for endpoint in (args.source, args.source_peer or args.source)]
        self.target = Relay(args.destination, self)
        self.bootstrap = None
        self.last_source = None
        self.armed = False
        self.published = False
        self.cut = asyncio.Event()
        self.loss_reason = None
        self.loss_at = None
        self.recovered_ms = None
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

    def backend_loss(self, reason):
        if self.published and not self.cut.is_set():
            self.loss_reason = reason
            self.loss_at = asyncio.get_running_loop().time()
            self.cut.set()
            self.count_connections()

    def switch(self, source):
        self.published = True
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
        if self.published:
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
    if args.source_admin or args.source_peer:
        if not args.source_peer or args.source_peer == args.source or len(set(args.source_admin or [])) != 2:
            raise ValueError("node disconnect needs a distinct --source-peer and two --source-admin URLs")
    if args.destination in (args.source, args.source_peer):
        raise ValueError("provide distinct source and destination backends")
    env = os.environ.copy()
    token = env.pop("YDB_CUTOVER_ADMIN_TOKEN", None)
    if args.source_admin and (not token or not re.fullmatch(r"[ -~]+", token)):
        raise ValueError("set YDB_CUTOVER_ADMIN_TOKEN to a valid Authorization header value")
    route = Route(args)
    async with AsyncExitStack() as stack:
        for relay in [*route.sources, route.target]:
            await relay.start(stack)
        # Use the real ID for direct Topic partition lookups. The extra source
        # address models a still-reachable peer in a cached discovery response.
        for relay in [*route.sources, route.target]:
            async with grpc.aio.insecure_channel(relay.endpoint) as channel:
                reply = await discovery.DiscoveryServiceStub(channel).ListEndpoints(
                    proto.ListEndpointsRequest(database=args.database), timeout=5
                )
                topology = proto.ListEndpointsResult()
                if not reply.operation.result.Unpack(topology) or not topology.endpoints:
                    raise RuntimeError(f"backend discovery failed: {reply.operation}")
                if route.node_disconnect:
                    candidates = [ep for ep in topology.endpoints if ep.port == relay.port]
                    if len(candidates) != 1:
                        candidates = [ep for ep in candidates if ep.address == relay.host]
                    if len(candidates) != 1:
                        raise ValueError(f"cannot identify {relay.endpoint} in backend discovery")
                    relay.node_id = candidates[0].node_id
                else:
                    relay.node_id = topology.endpoints[0].node_id + (relay is route.sources[1])

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
            env={**env, "YDB_ENDPOINT": f"127.0.0.1:{route.bootstrap.address}",
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
            if route.node_disconnect:
                route.published = True
                # Leave every relay and source listener open. Only ydbd cuts connections.
                await asyncio.gather(*(asyncio.to_thread(disconnect, url, args.database, token)
                                       for url in args.source_admin))
            elif args.probe.startswith("topic-"):
                route.switch(route.last_source)
            else:
                route.armed = True  # Cut the next unary RPC while it is in flight.
            await asyncio.wait_for(route.cut.wait(), timeout=10)
            deadline = (route.loss_at or asyncio.get_running_loop().time()) + args.timeout
            async with asyncio.timeout_at(deadline):
                while True:
                    await success(args.destination)
                    if route.stale:
                        raise AssertionError("SDK sent a new RPC to a cached source endpoint")
                    if route.discovered and route.reached:
                        if route.loss_at is not None:
                            route.recovered_ms = round((asyncio.get_running_loop().time() - route.loss_at) * 1000)
                        break
            if route.node_disconnect:
                # Observe additional RPCs after the first destination success.
                await asyncio.sleep(0.6)
                if route.stale:
                    raise AssertionError("SDK sent a new RPC to a cached source endpoint")
            if route.after_peak > route.before_peak:
                raise AssertionError(f"peak connections increased: {route.before_peak} -> {route.after_peak}")
        finally:
            if client.returncode is None:
                with suppress(ProcessLookupError):
                    client.kill()
            await client.wait()
            print(f"{args.client} {args.probe}: rediscovered={route.discovered}, "
                  f"destination_rpc={route.reached}, stale_rpc={route.stale}, "
                  f"peak_connections={route.before_peak}/{route.after_peak}, "
                  f"backend_loss={route.loss_reason}, recovered_ms={route.recovered_ms}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True, choices=sorted(CLIENTS))
    parser.add_argument("--source", required=True, help="disposable source ydbd host:port")
    parser.add_argument("--source-peer", help="second source data node host:port for a node-admin cutover")
    parser.add_argument("--source-admin", nargs=2, metavar=("URL1", "URL2"), help="source node monitoring URLs")
    parser.add_argument("--destination", required=True, help="disposable destination ydbd host:port")
    parser.add_argument("--database", default="/local")
    parser.add_argument("--probe", choices=["scheme", "bulk-upsert", "topic-read", "topic-write"], default="scheme")
    parser.add_argument("--timeout", type=float, default=4, help="maximum seconds after transport loss")
    asyncio.run(run(parser.parse_args()))
