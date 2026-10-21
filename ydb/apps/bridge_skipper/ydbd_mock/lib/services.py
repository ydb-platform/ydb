"""gRPC servicers: Discovery, Config, Monitoring, Bridge. One set per node.

Every method goes through the node's fault hook. Application errors are ready Operations
with a status, never gRPC errors. Methods the mock does not override, and services it does not
register, answer gRPC UNIMPLEMENTED and record an `rpc.unimplemented` event.
"""
import grpc

from ydb.apps.bridge_skipper.ydbd_mock.lib import config_yaml, health, model, operations
from ydb.public.api.grpc import ydb_config_v1_pb2
from ydb.public.api.grpc import ydb_config_v1_pb2_grpc
from ydb.public.api.grpc import ydb_discovery_v1_pb2
from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc
from ydb.public.api.grpc import ydb_monitoring_v1_pb2
from ydb.public.api.grpc import ydb_monitoring_v1_pb2_grpc
from ydb.public.api.grpc.draft import ydb_bridge_v1_pb2
from ydb.public.api.grpc.draft import ydb_bridge_v1_pb2_grpc
from ydb.public.api.protos import ydb_config_pb2
from ydb.public.api.protos import ydb_discovery_pb2
from ydb.public.api.protos import ydb_monitoring_pb2
from ydb.public.api.protos import ydb_status_codes_pb2
from ydb.public.api.protos.draft import ydb_bridge_pb2

AUTH_TICKET_HEADER = "x-ydb-auth-ticket"

DISCOVERY = ydb_discovery_v1_pb2.DESCRIPTOR.services_by_name["DiscoveryService"].full_name
CONFIG = ydb_config_v1_pb2.DESCRIPTOR.services_by_name["ConfigService"].full_name
MONITORING = ydb_monitoring_v1_pb2.DESCRIPTOR.services_by_name["MonitoringService"].full_name
BRIDGE = ydb_bridge_v1_pb2.DESCRIPTOR.services_by_name["BridgeService"].full_name


def operation_status(response) -> str:
    """Status name of a response envelope, for rpc.end events."""
    try:
        return ydb_status_codes_pb2.StatusIds.StatusCode.Name(response.operation.status)
    except (AttributeError, ValueError):
        return "OK"


class _Servicer:
    SERVICE = ""

    def __init__(self, instance, runtime, tls: bool = False) -> None:
        self.inst = instance
        self.rt = runtime
        self.node = runtime.node
        self.tls = tls

    async def _run(self, method: str, context, call, summary: str = ""):
        return await self.rt.run("/%s/%s" % (self.SERVICE, method), context, call, summary, operation_status)


class DiscoveryServicer(_Servicer, ydb_discovery_v1_pb2_grpc.DiscoveryServiceServicer):
    SERVICE = DISCOVERY

    async def ListEndpoints(self, request, context):
        async def call():
            node = self.node
            info = ydb_discovery_pb2.EndpointInfo(
                address=node.host,
                port=node.grpc_port,
                load_factor=0.0,
                ssl=self.tls,
                location=node.data_center,
                node_id=node.node_id,
                bridge_pile_name=node.pile_name,
            )
            result = ydb_discovery_pb2.ListEndpointsResult(endpoints=[info], self_location=node.data_center)
            state = self.inst.replica.state
            for i, name in enumerate(self.inst.names):
                result.pile_states.add(pile_name=name, state=model.public_state(state, i))
            return ydb_discovery_pb2.ListEndpointsResponse(operation=operations.ok(result))

        return await self._run("ListEndpoints", context, call, "database=%r" % request.database)


class ConfigServicer(_Servicer, ydb_config_v1_pb2_grpc.ConfigServiceServicer):
    SERVICE = CONFIG

    async def FetchConfig(self, request, context):
        async def call():
            entry = ydb_config_pb2.ConfigEntry(config=config_yaml.render(self.inst.topo, 1))
            entry.identity.version = 1
            entry.identity.cluster = self.inst.topo.cluster_name
            entry.identity.main.SetInParent()
            result = ydb_config_pb2.FetchConfigResult(config=[entry])
            return ydb_config_pb2.FetchConfigResponse(operation=operations.ok(result))

        return await self._run("FetchConfig", context, call)


class MonitoringServicer(_Servicer, ydb_monitoring_v1_pb2_grpc.MonitoringServiceServicer):
    SERVICE = MONITORING

    async def SelfCheck(self, request, context):
        async def call():
            result = health.build(self.inst, self.node)
            return ydb_monitoring_pb2.SelfCheckResponse(operation=operations.ok(result))

        summary = "verbose=%s merge=%s cache=%s" % (
            request.return_verbose_status, request.merge_records, not request.do_not_cache)
        return await self._run("SelfCheck", context, call, summary)


class BridgeServicer(_Servicer, ydb_bridge_v1_pb2_grpc.BridgeServiceServicer):
    SERVICE = BRIDGE

    async def GetClusterState(self, request, context):
        async def call():
            state = self.inst.replica.state
            result = ydb_bridge_pb2.GetClusterStateResult(generation=state.generation)
            for i, name in enumerate(self.inst.names):
                result.pile_states.add(pile_name=name, state=model.public_state(state, i))
            return ydb_bridge_pb2.GetClusterStateResponse(operation=operations.ok(result))

        return await self._run("GetClusterState", context, call)

    async def UpdateClusterState(self, request, context):
        updates = [(u.pile_name, u.state) for u in request.updates]
        quorum = list(request.quorum_piles)
        summary = "updates=[%s] quorum=%s" % (
            ", ".join("%s=%s" % (n, model.public_name(s)) for n, s in updates), quorum)

        async def call():
            token = self.inst.policies.admin_token
            if token:
                metadata = {k.lower(): v for k, v in (context.invocation_metadata() or ())}
                if metadata.get(AUTH_TICKET_HEADER) != token:
                    return ydb_bridge_pb2.UpdateClusterStateResponse(operation=operations.error(
                        model.UNAUTHORIZED, "Bridge operations require administrator privileges"))
            self.rt.track_update_reply(context)
            outcome = await self.inst.admin_update(updates, quorum)
            if outcome.error is not None:
                return ydb_bridge_pb2.UpdateClusterStateResponse(
                    operation=operations.error(outcome.error.status, outcome.error.message))
            return ydb_bridge_pb2.UpdateClusterStateResponse(
                operation=operations.ok(ydb_bridge_pb2.UpdateClusterStateResult()))

        return await self._run("UpdateClusterState", context, call, summary)


def _fill_unimplemented(cls, base, service: str) -> None:
    """Replace every RPC of `base` that `cls` does not override with a recording UNIMPLEMENTED handler."""
    for name in dir(base):
        if name.startswith("_") or not callable(getattr(base, name)):
            continue
        if name in cls.__dict__:
            continue

        def make(method_name):
            async def unimplemented(self, request, context):
                self.rt.events.append(
                    "rpc.unimplemented", node=self.node.label, method="/%s/%s" % (service, method_name),
                    registered=True)
                await context.abort(grpc.StatusCode.UNIMPLEMENTED, "mock: %s is not implemented" % method_name)

            unimplemented.__name__ = method_name
            return unimplemented

        setattr(cls, name, make(name))


_fill_unimplemented(DiscoveryServicer, ydb_discovery_v1_pb2_grpc.DiscoveryServiceServicer, DISCOVERY)
_fill_unimplemented(ConfigServicer, ydb_config_v1_pb2_grpc.ConfigServiceServicer, CONFIG)
_fill_unimplemented(MonitoringServicer, ydb_monitoring_v1_pb2_grpc.MonitoringServiceServicer, MONITORING)
_fill_unimplemented(BridgeServicer, ydb_bridge_v1_pb2_grpc.BridgeServiceServicer, BRIDGE)


class UnregisteredServiceInterceptor(grpc.aio.ServerInterceptor):
    """Calls for which the server has no handler: answer UNIMPLEMENTED and record the event."""

    def __init__(self, events, node_label: str) -> None:
        self._events = events
        self._node = node_label

    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)
        if handler is not None:
            return handler
        method = handler_call_details.method
        events = self._events
        node = self._node

        async def abort(request, context):
            events.append("rpc.unimplemented", node=node, method=method, registered=False)
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, "mock: %s is not implemented" % method)

        return grpc.unary_unary_rpc_method_handler(abort)


def register(server: "grpc.aio.Server", instance, runtime, tls: bool = False) -> None:
    ydb_discovery_v1_pb2_grpc.add_DiscoveryServiceServicer_to_server(DiscoveryServicer(instance, runtime, tls), server)
    ydb_config_v1_pb2_grpc.add_ConfigServiceServicer_to_server(ConfigServicer(instance, runtime, tls), server)
    ydb_monitoring_v1_pb2_grpc.add_MonitoringServiceServicer_to_server(
        MonitoringServicer(instance, runtime, tls), server)
    ydb_bridge_v1_pb2_grpc.add_BridgeServiceServicer_to_server(BridgeServicer(instance, runtime, tls), server)
