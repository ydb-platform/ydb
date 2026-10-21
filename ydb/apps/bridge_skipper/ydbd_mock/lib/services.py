"""gRPC servicers: Discovery, Config, Monitoring, Bridge. One set per node.

Every method goes through the node's fault hook. Application errors are ready Operations
with a status, never gRPC errors. Methods the mock does not override, and services it does not
register, answer gRPC UNIMPLEMENTED and record an `rpc.unimplemented` event.
"""
import base64
import asyncio
import grpc
from google.protobuf import json_format

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

    async def _run(self, method: str, context, call, summary: str = "", request=None):
        module = {DISCOVERY: ydb_discovery_pb2, CONFIG: ydb_config_pb2,
                  MONITORING: ydb_monitoring_pb2, BRIDGE: ydb_bridge_pb2}[self.SERVICE]

        async def dispatch():
            mutation = self.SERVICE == BRIDGE and method != "GetClusterState"
            if self._authorized(context, mutation=mutation):
                override = self.rt.take_rpc_override(method)
                if override is not None:
                    raw = override.pop("_raw_result", None)
                    response = json_format.ParseDict(override, getattr(module, method + "Response")())
                    if raw is not None:
                        response.operation.result.type_url = raw["type_url"]
                        response.operation.result.value = base64.b64decode(raw["value_base64"])
                    return response
            return await call()

        run = self.rt.run("/%s/%s" % (self.SERVICE, method), context, dispatch, summary, operation_status)
        timeout = 0.0
        if request is not None and hasattr(request, "operation_params"):
            duration = request.operation_params.operation_timeout
            timeout = duration.seconds + duration.nanos / 1e9
        if timeout > 0:
            try:
                return await asyncio.wait_for(run, timeout)
            except asyncio.TimeoutError:
                return getattr(module, method + "Response")(operation=operations.error(
                    model.TIMEOUT, "Operation timeout"))
        return await run

    def _authorized(self, context, mutation=False):
        token = self.inst.policies.admin_token if mutation else self.inst.policies.read_token
        metadata = {k.lower(): v for k, v in (context.invocation_metadata() or ())}
        if mutation:
            names = self.inst.policies.admin_certificate_cn
            if names:
                certificates = context.auth_context().get("x509_common_name", ())
                if any(c.decode("utf-8", errors="replace") in names for c in certificates):
                    return True
                return bool(token) and metadata.get(AUTH_TICKET_HEADER) == token
        return not token or metadata.get(AUTH_TICKET_HEADER) == token

    async def _read(self, method, context, result, response_type):
        if not self._authorized(context):
            return response_type(operation=operations.error(model.UNAUTHORIZED, "Authentication token is required"))
        override = self.rt.response_override(method)
        if override is not None:
            result = json_format.ParseDict(override, type(result)())
        # The message is already a snapshot; a release cannot make this reply newer.
        response = response_type(operation=operations.ok(result))
        await self.rt.checkpoint("/%s/%s" % (self.SERVICE, method), "after_snapshot", context)
        return response

    async def _checkpoint(self, method, stage, context):
        await self.rt.checkpoint("/%s/%s" % (self.SERVICE, method), stage, context)


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
            if state.bridged:
                for name in state.registered_names(self.inst.names):
                    result.pile_states.add(pile_name=name, state=model.public_state(state, self.inst.names.index(name)))
            return await self._read("ListEndpoints", context, result, ydb_discovery_pb2.ListEndpointsResponse)

        return await self._run("ListEndpoints", context, call, "database=%r" % request.database, request)


class ConfigServicer(_Servicer, ydb_config_v1_pb2_grpc.ConfigServiceServicer):
    SERVICE = CONFIG

    async def FetchConfig(self, request, context):
        async def call():
            version = self.inst.state.revision + 1
            entry = ydb_config_pb2.ConfigEntry(config=config_yaml.render(self.inst.topo, version, self.inst.state))
            entry.identity.version = version
            entry.identity.cluster = self.inst.topo.cluster_name
            entry.identity.main.SetInParent()
            result = ydb_config_pb2.FetchConfigResult(config=[entry])
            return await self._read("FetchConfig", context, result, ydb_config_pb2.FetchConfigResponse)

        return await self._run("FetchConfig", context, call, request=request)


class MonitoringServicer(_Servicer, ydb_monitoring_v1_pb2_grpc.MonitoringServiceServicer):
    SERVICE = MONITORING

    async def SelfCheck(self, request, context):
        async def call():
            result = health.build(self.inst, self.node)
            return await self._read("SelfCheck", context, result, ydb_monitoring_pb2.SelfCheckResponse)

        summary = "verbose=%s merge=%s cache=%s" % (
            request.return_verbose_status, request.merge_records, not request.do_not_cache)
        return await self._run("SelfCheck", context, call, summary, request)


class BridgeServicer(_Servicer, ydb_bridge_v1_pb2_grpc.BridgeServiceServicer):
    SERVICE = BRIDGE

    async def GetClusterState(self, request, context):
        async def call():
            state = self.inst.replica.state
            if not self._authorized(context):
                return ydb_bridge_pb2.GetClusterStateResponse(operation=operations.error(
                    model.UNAUTHORIZED, "Authentication token is required"))
            if not state.self_management:
                return ydb_bridge_pb2.GetClusterStateResponse(operation=operations.error(
                    model.UNSUPPORTED, "Bridge operations require self-management mode to be enabled"))
            if not state.bridged:
                return ydb_bridge_pb2.GetClusterStateResponse(operation=operations.error(
                    model.NOT_FOUND, "Bridge cluster state is not configured"))
            result = ydb_bridge_pb2.GetClusterStateResult(
                generation=state.generation, conversion_in_progress=state.conversion_in_progress)
            for name in state.registered_names(self.inst.names):
                result.pile_states.add(pile_name=name, state=model.public_state(state, self.inst.names.index(name)))
            if state.pending_pile_operation:
                pending = state.pending_pile_operation
                result.pending_pile_operation.pile_name = pending["pile_name"]
                result.pending_pile_operation.kind = ydb_bridge_pb2.PendingPileOperation.Kind.Value(pending["kind"])
                result.pending_pile_operation.error_reason = pending.get("error_reason", "")
            return await self._read("GetClusterState", context, result, ydb_bridge_pb2.GetClusterStateResponse)

        return await self._run("GetClusterState", context, call, request=request)

    async def UpdateClusterState(self, request, context):
        updates = [(u.pile_name, u.state) for u in request.updates]
        quorum = list(request.quorum_piles)
        summary = "updates=[%s] quorum=%s" % (
            ", ".join("%s=%s" % (n, model.public_name(s)) for n, s in updates), quorum)

        async def call():
            if not self._authorized(context, mutation=True):
                return ydb_bridge_pb2.UpdateClusterStateResponse(operation=operations.error(
                    model.UNAUTHORIZED, "Bridge operations require administrator privileges"))
            self.rt.track_update_reply(context)
            outcome = await self.inst.admin_update(updates, quorum, accepted=lambda: self._checkpoint(
                "UpdateClusterState", "after_acceptance", context))
            if outcome.error is not None:
                return ydb_bridge_pb2.UpdateClusterStateResponse(
                    operation=operations.error(outcome.error.status, outcome.error.message))
            await self._checkpoint("UpdateClusterState", "after_commit", context)
            return ydb_bridge_pb2.UpdateClusterStateResponse(
                operation=operations.ok(ydb_bridge_pb2.UpdateClusterStateResult()))

        return await self._run("UpdateClusterState", context, call, summary, request)

    async def _lifecycle(self, method, kind, request, context):
        response_type = getattr(ydb_bridge_pb2, method + "Response")
        result_type = getattr(ydb_bridge_pb2, method + "Result")

        async def call():
            if not self._authorized(context, mutation=True):
                return response_type(operation=operations.error(
                    model.UNAUTHORIZED, "Bridge operations require administrator privileges"))
            self.rt.track_update_reply(context)
            outcome = await self.inst.admin_lifecycle(
                kind, getattr(request, "pile_name", ""),
                accepted=lambda: self._checkpoint(method, "after_acceptance", context),
                committed=lambda: self._checkpoint(method, "after_commit", context))
            if outcome.error:
                return response_type(operation=operations.error(outcome.error.status, outcome.error.message))
            return response_type(operation=operations.ok(result_type()))

        return await self._run(method, context, call, "pile=%r" % getattr(request, "pile_name", ""), request)

    async def ConvertToBridge(self, request, context):
        return await self._lifecycle("ConvertToBridge", "convert", request, context)

    async def AddPile(self, request, context):
        return await self._lifecycle("AddPile", "add", request, context)

    async def RemovePile(self, request, context):
        return await self._lifecycle("RemovePile", "remove", request, context)


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
