"""Test double for the managed L7 route. Each RPC is forwarded to a real ydbd."""
import asyncio
from contextlib import asynccontextmanager

import grpc


EDS = "/envoy.service.endpoint.v3.EndpointDiscoveryService/StreamEndpoints"
LIST = "/Ydb.Discovery.V1.DiscoveryService/ListEndpoints"
WRITE = "/Ydb.Topic.V1.TopicService/StreamWrite"
GENERATION = "x-ydb-route-generation"
MODE = "x-ydb-route-mode"
FLOOR = "x-ydb-min-route-generation"


class Balancer(grpc.GenericRpcHandler):
    def __init__(self, database, backends):
        self.database = database
        self.backends = backends
        self.generation = 1
        self.target = 0
        self.watches = {}
        self.subscriptions = asyncio.Queue()
        self.unary_requests = []
        self.topic_peers = []

    @asynccontextmanager
    async def serve(self):
        server = grpc.aio.server()
        server.add_generic_rpc_handlers((self,))
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            yield "127.0.0.1:%d" % port
        finally:
            await server.stop(0)

    def switch(self, target):
        self.target = target
        self.generation += 1
        for call in self.watches:
            call.cancel()

    def service(self, details):
        if details.method == EDS:
            return grpc.stream_stream_rpc_method_handler(self.watch)
        if details.method == LIST:
            return grpc.unary_unary_rpc_method_handler(self.list_endpoints)
        if details.method == WRITE:
            return grpc.stream_stream_rpc_method_handler(self.write)
        return None

    async def check_route(self, context, generation):
        headers = dict(context.invocation_metadata())
        if headers.get("x-ydb-database") != self.database:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "missing routing database")
        if int(headers.get(FLOOR, "0")) > generation:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "worker is behind the requested generation")

    def upstream_headers(self, context):
        return [(k, v) for k, v in context.invocation_metadata() if k in ("x-ydb-database", "x-ydb-auth-ticket")]

    async def watch(self, requests, context):
        target, generation = self.target, self.generation
        await self.check_route(context, generation)
        call = self.backends[target].stream_stream(EDS)(requests, metadata=self.upstream_headers(context))
        self.watches[call] = generation
        self.subscriptions.put_nowait((target, context.peer(), dict(context.invocation_metadata())))
        try:
            await context.send_initial_metadata(((GENERATION, str(generation)), (MODE, "managed")))
            async for response in call:
                yield response
        except (asyncio.CancelledError, grpc.aio.AioRpcError) as error:
            if generation != self.generation:
                await context.abort(
                    grpc.StatusCode.UNAVAILABLE, "route changed", trailing_metadata=((GENERATION, str(self.generation)),)
                )
            if isinstance(error, grpc.aio.AioRpcError):
                await context.abort(error.code(), error.details())
            raise
        finally:
            self.watches.pop(call, None)
            call.cancel()

    async def list_endpoints(self, request, context):
        # Legacy clients may set only ListEndpointsRequest.database.
        target, generation = self.target, self.generation
        metadata = self.upstream_headers(context)
        if dict(metadata).get("x-ydb-database"):
            await self.check_route(context, generation)
        await context.send_initial_metadata(((GENERATION, str(generation)), (MODE, "managed")))
        self.unary_requests.append((target, context.peer(), dict(context.invocation_metadata())))
        try:
            return await self.backends[target].unary_unary(LIST)(request, metadata=metadata, timeout=5)
        except grpc.aio.AioRpcError as error:
            await context.abort(error.code(), error.details())

    async def write(self, requests, context):
        self.topic_peers.append(context.peer())
        call = self.backends[self.target].stream_stream(WRITE)(requests, metadata=self.upstream_headers(context))
        try:
            async for response in call:
                yield response
        except grpc.aio.AioRpcError as error:
            await context.abort(error.code(), error.details())
        finally:
            call.cancel()
