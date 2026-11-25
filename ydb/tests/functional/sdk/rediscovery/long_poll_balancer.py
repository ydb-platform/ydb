"""Test double for an AWACS route backed by real unary YDB discovery."""
import asyncio
from contextlib import asynccontextmanager

import grpc


LIST = "/Ydb.Discovery.V1.DiscoveryService/ListEndpoints"
OPT_IN = "x-ydb-discovery-long-poll"
CONDITION = "x-ydb-if-route-generation"
GENERATION = "x-ydb-route-generation"


class Balancer(grpc.GenericRpcHandler):
    def __init__(self, database, backends, hold=30):
        self.database = database
        self.backends = backends
        self.hold = hold
        self.generation = 1
        self.target = 0
        self.changed = asyncio.Event()
        self.subscriptions = asyncio.Queue()

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
        self.changed.set()
        self.changed = asyncio.Event()

    def service(self, details):
        if details.method == LIST:
            return grpc.unary_unary_rpc_method_handler(self.list_endpoints)
        return None

    async def list_endpoints(self, request, context):
        metadata = list(context.invocation_metadata())
        headers = dict(metadata)
        managed = headers.get(OPT_IN) == "1"
        known = headers.get(CONDITION)
        if managed:
            if headers.get("x-ydb-database") != self.database:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "incorrect database")
            if known is not None:
                if not known.isascii() or not known.isdecimal() or int(known) > self.generation:
                    await context.abort(grpc.StatusCode.UNAVAILABLE, "invalid or future generation")
                if int(known) == self.generation:
                    self.subscriptions.put_nowait((self.target, context.peer(), headers))
                    try:
                        await asyncio.wait_for(self.changed.wait(), self.hold)
                    except asyncio.TimeoutError:
                        pass
        generation, backend = self.generation, self.backends[self.target]
        forwards = [(k, v) for k, v in metadata if k in ("x-ydb-database", "x-ydb-auth-ticket")]
        try:
            result = await backend.unary_unary(LIST)(request, metadata=forwards, timeout=5)
        except grpc.aio.AioRpcError as error:
            await context.abort(error.code(), error.details())
        if managed:
            if self.generation != generation:
                await context.abort(grpc.StatusCode.UNAVAILABLE, "route changed during lookup")
            context.set_trailing_metadata(((GENERATION, str(generation)),))
        return result
