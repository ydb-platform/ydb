"""HTTP JSON control API and peer API of one instance (aiohttp, on the control port)."""
import base64
import binascii
import json
import os
import typing
from google.protobuf import json_format

from aiohttp import web

from ydb.apps.bridge_skipper.ydbd_mock.lib import model
from ydb.apps.bridge_skipper.ydbd_mock.lib.peers import PEER_OPS, PeerBlocked
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import TopologyError

SELF_CHECK_VALUES = ("GOOD", "DEGRADED", "MAINTENANCE_REQUIRED", "EMERGENCY")
NODE_OPS = ("storage", "connectivity", "down", "unavailable", "silence", "delay", "self_check", "clear")


class ApiError(Exception):
    def __init__(self, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


@web.middleware
async def _errors(request, handler):
    try:
        return await handler(request)
    except ApiError as e:
        return web.json_response({"error": str(e)}, status=e.status)
    except (TopologyError, ValueError, KeyError, TypeError) as e:
        return web.json_response({"error": "%s: %s" % (type(e).__name__, e)}, status=400)


async def _body(request) -> dict:
    try:
        data = await request.json()
    except (ValueError, json.JSONDecodeError):
        raise ApiError("body must be JSON")
    if not isinstance(data, dict):
        raise ApiError("body must be a JSON object")
    return data


def build_app(process) -> web.Application:
    """`process` is a MockProcess (instance.py): it owns the PileInstance, the node servers and the events."""
    inst = process.inst
    names = inst.names
    app = web.Application(middlewares=[_errors])

    def pile_index(name) -> int:
        if name not in names:
            raise ApiError("unknown pile %r, known: %s" % (name, ", ".join(names)), 404)
        return names.index(name)

    def pile_list(value) -> typing.List[str]:
        if not isinstance(value, list) or not all(isinstance(x, str) for x in value):
            raise ApiError("expected a list of pile names")
        for name in value:
            pile_index(name)
        return value

    # ----- views -----

    async def get_state(request):
        # the pid lets the supervisor tell its own child from a stranger that holds the port
        return web.json_response(dict(inst.state_json(), pid=os.getpid()))

    async def get_events(request):
        q = request.query
        try:
            since = int(q.get("since", 0))
            limit = int(q["limit"]) if "limit" in q else None
            after_ts = float(q["after_ts"]) if "after_ts" in q else None
        except ValueError:
            raise ApiError("since and limit are integers, after_ts is a number")
        events = inst.events.query(since=since, after_ts=after_ts, kind=q.get("kind"), grep=q.get("grep"),
                                   limit=limit)
        return web.json_response({"pile": inst.name, "last_seq": inst.events.last_seq, "events": events})

    # ----- control -----

    def target_nodes(body) -> typing.List[int]:
        """Indexes (within this pile) of the targeted nodes."""
        nodes = inst.pile_info.nodes
        if body.get("all"):
            return [n.index_in_pile for n in nodes]
        if "label" in body:
            for n in nodes:
                if n.label == body["label"]:
                    return [n.index_in_pile]
            raise ApiError("no node with label %r in pile %s" % (body["label"], inst.name), 404)
        if "node" in body:
            for n in nodes:
                if n.node_id == body["node"]:
                    return [n.index_in_pile]
            raise ApiError("no node %r in pile %s" % (body["node"], inst.name), 404)
        raise ApiError('target a node with "node", "label" or "all"')

    async def post_control(request):
        body = await _body(request)
        op = body.get("op")
        if op not in NODE_OPS:
            raise ApiError("unknown op %r, known: %s" % (op, ", ".join(NODE_OPS)))
        targets = target_nodes(body)
        # validate before touching anything
        if op in ("storage", "connectivity"):
            piles = pile_list(body.get("piles", []))
        if op == "self_check":
            value = body.get("value")
            if value is not None and value not in SELF_CHECK_VALUES:
                raise ApiError("self_check value must be one of %s or null" % ", ".join(SELF_CHECK_VALUES))
        if op == "delay":
            try:
                seconds = float(body.get("seconds", 0))
            except (TypeError, ValueError):
                raise ApiError("delay needs a number of seconds")
            if seconds < 0:
                raise ApiError("delay must not be negative")
        methods = body.get("methods")
        if op in ("delay", "silence") and methods is not None and not (
                isinstance(methods, list) and all(isinstance(m, str) for m in methods)):
            raise ApiError("methods must be a list of names")
        if op in ("down", "unavailable", "silence"):
            on = body.get("on", True)
            if not isinstance(on, bool):
                raise ApiError('"on" must be a boolean')
            max_seconds = body.get("max")
            if max_seconds is not None:
                try:
                    max_seconds = float(max_seconds)
                except (TypeError, ValueError):
                    raise ApiError('"max" must be a number')

        for i in targets:
            node = inst.pile_info.nodes[i]
            faults = inst.node_faults[i]
            runtime = process.runtimes[i]
            if op == "storage":
                faults.blame_storage = set(piles)
            elif op == "connectivity":
                faults.blame_connectivity = set(piles)
            elif op == "down":
                await process.set_node_down(i, on)
            elif op == "unavailable":
                faults.unavailable = on
            elif op == "silence":
                runtime.set_silence(on, max_seconds if on else None, methods)
            elif op == "delay":
                faults.delay = seconds
                faults.delay_methods = set(methods) if methods else None
            elif op == "self_check":
                faults.self_check = body.get("value")
            elif op == "clear":
                was_down = faults.down
                runtime.clear()
                if was_down:
                    await process.set_node_down(i, False)
            inst.events.append("fault", node=node.label, op=op,
                               args={k: v for k, v in body.items() if k not in ("op", "node", "label", "all")})
        inst.on_nodes_changed()
        return web.json_response(inst.state_json())

    async def post_link(request):
        body = await _body(request)
        current = {names[p] for p in inst.reach.blocked}
        if "set" in body:
            wanted = set(pile_list(body["set"]))
        else:
            wanted = set(current)
            if "block" in body:
                wanted |= set(pile_list(body["block"]))
            if "unblock" in body:
                wanted -= set(pile_list(body["unblock"]))
            if "block" not in body and "unblock" not in body:
                raise ApiError('use "block", "unblock" or "set"')
        inst.set_blocked(pile_index(n) for n in wanted)
        return web.json_response(inst.state_json())

    async def post_replica(request):
        body = await _body(request)
        if body.get("clear"):
            inst.clear_replica_override()
            return web.json_response(inst.state_json())
        states = body.get("states", {})
        if not isinstance(states, dict):
            raise ApiError('"states" must map pile name to public state')
        public = {}
        for name, value in states.items():
            pile_index(name)
            try:
                public[name] = model.public_value(value)
            except ValueError:
                raise ApiError("unknown pile state %r" % (value,))
        generation = body.get("generation")
        if generation is not None and (not isinstance(generation, int) or generation < 0):
            raise ApiError('"generation" must be a non-negative integer')
        inst.override_replica(public, generation, bool(body.get("detach", False)))
        return web.json_response(inst.state_json())

    async def post_timings(request):
        body = await _body(request)
        inst.timings.update(body)
        inst.events.append("timings", values=body)
        inst.on_nodes_changed()
        return web.json_response(inst.state_json())

    async def post_policies(request):
        body = await _body(request)
        inst.policies.update(body)
        inst.events.append("policies", values={k: v for k, v in body.items() if k not in ("admin_token", "read_token")})
        inst.on_nodes_changed()
        return web.json_response(inst.state_json())

    async def post_topology(request):
        outcome = await inst.configure_topology(await _body(request))
        if outcome.error:
            raise ApiError(outcome.error.message, 409)
        return web.json_response(inst.state_json())

    async def post_followups(request):
        inst.control_followups(await _body(request))
        return web.json_response(inst.state_json())

    async def post_failpoints(request):
        body = await _body(request)
        targets = target_nodes(body)
        method, stage, action = body.get("method"), body.get("stage"), body.get("action", "hold")
        # Validate once before changing any selected node.
        if not isinstance(method, str) or not method.startswith("/") or method.count("/") != 2 or stage not in (
                "after_snapshot", "after_acceptance", "after_commit"):
            raise ApiError("failpoint requires full RPC method and a known stage")
        if action not in ("hold", "unavailable", "release"):
            raise ApiError("action must be hold, unavailable or release")
        once = body.get("once", False)
        if not isinstance(once, bool):
            raise ApiError("once must be a boolean")
        for i in targets:
            runtime = process.runtimes[i]
            if action == "release":
                runtime.release_failpoint(method, stage)
            else:
                runtime.configure_failpoint(method, stage, action, once=once)
        return web.json_response({"nodes": [{"label": process.runtimes[i].node.label,
                                            "failpoints": process.runtimes[i].failpoints_json()} for i in targets]})

    async def post_response_override(request):
        body = await _body(request)
        targets = target_nodes(body)
        method, result = body.get("method"), body.get("result")
        from ydb.apps.bridge_skipper.ydbd_mock.lib.services import (
            ydb_bridge_pb2, ydb_monitoring_pb2, ydb_config_pb2, ydb_discovery_pb2)
        types = {"GetClusterState": ydb_bridge_pb2.GetClusterStateResult,
                 "SelfCheck": ydb_monitoring_pb2.SelfCheckResult,
                 "FetchConfig": ydb_config_pb2.FetchConfigResult,
                 "ListEndpoints": ydb_discovery_pb2.ListEndpointsResult}
        if method not in types or (result is not None and not isinstance(result, dict)):
            raise ApiError("response override requires a supported read method and an object or null result")
        if result is not None:
            try:
                json_format.ParseDict(result, types[method]())
            except json_format.ParseError as e:
                raise ApiError(str(e))
        for i in targets:
            process.runtimes[i].set_response_override(method, result)
        return web.json_response(inst.state_json())

    async def post_rpc_override(request):
        body = await _body(request)
        targets = target_nodes(body)
        method, response, once = body.get("method"), body.get("response"), body.get("once", False)
        from ydb.apps.bridge_skipper.ydbd_mock.lib.services import (
            ydb_bridge_pb2, ydb_monitoring_pb2, ydb_config_pb2, ydb_discovery_pb2)
        modules = {"GetClusterState": ydb_bridge_pb2, "UpdateClusterState": ydb_bridge_pb2,
                   "ConvertToBridge": ydb_bridge_pb2, "AddPile": ydb_bridge_pb2, "RemovePile": ydb_bridge_pb2,
                   "SelfCheck": ydb_monitoring_pb2, "FetchConfig": ydb_config_pb2,
                   "ListEndpoints": ydb_discovery_pb2}
        if method not in modules or not isinstance(once, bool) or (
                response is not None and not isinstance(response, dict)):
            raise ApiError("rpc override needs a supported method, response object or null, and boolean once")
        if response is not None:
            try:
                json_format.ParseDict(response, getattr(modules[method], method + "Response")())
            except json_format.ParseError as e:
                raise ApiError(str(e))
            raw = body.get("raw_result")
            if raw is not None:
                if not isinstance(raw, dict) or not isinstance(raw.get("type_url"), str) or not isinstance(
                        raw.get("value_base64"), str):
                    raise ApiError("raw_result needs type_url and value_base64 strings")
                try:
                    base64.b64decode(raw["value_base64"], validate=True)
                except (ValueError, binascii.Error):
                    raise ApiError("raw_result.value_base64 must be valid base64")
                response = dict(response, _raw_result=dict(raw))
        for i in targets:
            process.runtimes[i].set_rpc_override(method, response, once=once)
        return web.json_response(inst.state_json())

    async def post_reset_prepare(request):
        inst.prepare_reset()
        return web.json_response(inst.state_json())

    async def post_reset_cancel(request):
        inst.cancel_reset()
        return web.json_response(inst.state_json())

    async def post_reset_local(request):
        body = await _body(request)
        generation = body.get("generation")
        if not isinstance(generation, int) or generation < 1:
            raise ApiError('"generation" must be a positive integer')
        # nodes that were down come back up
        inst.reset_local(generation)
        for runtime in process.runtimes:
            runtime.clear()
        await process.reconcile_listeners(grace=0.0)
        return web.json_response(inst.state_json())

    async def post_scenario(request):
        body = await _body(request)
        inst.events.append("scenario", **{k: v for k, v in body.items() if k != "kind"})
        return web.json_response({"ok": True})

    # ----- peer API -----

    async def peer(request):
        op = request.match_info["op"]
        if op not in PEER_OPS:
            raise ApiError("unknown peer op %r" % op, 404)
        if request.method == "GET":
            payload = {}
            sender = request.query.get("from")
        else:
            payload = await _body(request)
            sender = payload.get("from")
        if sender not in names:
            raise ApiError("unknown sender %r" % (sender,))
        try:
            reply = await inst.handle_peer(op, names.index(sender), payload)
        except PeerBlocked:
            return web.json_response({"error": "blocked"}, status=503)
        return web.json_response(reply)

    app.router.add_get("/state", get_state)
    app.router.add_get("/events", get_events)
    app.router.add_post("/control", post_control)
    app.router.add_post("/link", post_link)
    app.router.add_post("/replica", post_replica)
    app.router.add_post("/timings", post_timings)
    app.router.add_post("/policies", post_policies)
    app.router.add_post("/topology", post_topology)
    app.router.add_post("/followups", post_followups)
    app.router.add_post("/failpoints", post_failpoints)
    app.router.add_post("/response-override", post_response_override)
    app.router.add_post("/rpc-override", post_rpc_override)
    app.router.add_post("/reset-prepare", post_reset_prepare)
    app.router.add_post("/reset-cancel", post_reset_cancel)
    app.router.add_post("/reset-local", post_reset_local)
    app.router.add_post("/scenario", post_scenario)
    app.router.add_route("*", "/peer/{op}", peer)
    return app
