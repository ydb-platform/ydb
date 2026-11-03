"""Existing monitoring/log gates for physical replication role placement."""

import re

import requests

from ydb.tests.library.compatibility.fixtures import current_binary_path
from ydb.tests.library.replication_credentials import ADMIN, RPC_TIMEOUT


def monitor(node, path, params, *, method="get", timeout=RPC_TIMEOUT):
    response = requests.request(method, f"http://{node.host}:{node.mon_port}{path}", params=params,
                                headers={"Authorization": ADMIN}, timeout=timeout)
    try:
        response.raise_for_status()
    except requests.HTTPError as error:
        error.add_note(f"Monitoring response: {response.text[:2048]}; final_url={response.url}; "
                       f"redirects={[(item.status_code, item.url, item.headers.get('Location')) for item in response.history]}")
        raise
    return response.json()


def viewer_path(process, resource):
    # Use the root routes qualified by the native monitoring preflight.
    # Both binaries also register canonical routes asynchronously at startup.
    prefix = "/viewer/" if process.binary_path == current_binary_path else "/viewer/json/"
    return prefix + resource


def actual_node_id(destination, process):
    def ready(timeout):
        assert process.is_alive(), "Process exited before whiteboard qualification"
        root = destination.cluster.nodes[1]
        data = monitor(root, viewer_path(root, "sysinfo"),
                       {"database": destination.database_path, "direct": 1, "alive": 1,
                        "static": 0, "enums": "true"}, timeout=timeout)
        def endpoint_matches(entry, name, port):
            return any(endpoint.get("Name") == name
                       and endpoint.get("Address", "").rsplit(":", 1)[-1] == str(port)
                       for endpoint in entry.get("Endpoints", []))
        matches = [entry for entry in data.get("SystemStateInfo", [])
                   if endpoint_matches(entry, "grpc", process.port)
                   and endpoint_matches(entry, "http-mon", process.mon_port)]
        assert len(matches) <= 1, "Multiple live nodes claim the process endpoints"
        if not matches:
            return None
        entry = matches[0]
        # Query the already-running root's whiteboard endpoint. This avoids the
        # starting slot's asynchronous HTTP-handler registration; exact public
        # endpoint matching and live-executable manifests qualify its NodeId.
        destination.record("process-node-id", process=process.node_id, endpoint=process.endpoint,
                           binary=process.binary_path, observer=root.endpoint, sysinfo=entry)
        return int(entry["NodeId"])
    return destination.poll("actual dynamic process NodeId", ready)


def controller_location(destination, tablet, timeout=RPC_TIMEOUT):
    data = monitor(destination.cluster.nodes[1], viewer_path(destination.cluster.nodes[1], "tabletinfo"),
                   {"database": destination.database_path, "filter": f"(TabletId={tablet})", "enums": "true"},
                   timeout=timeout)
    live = [entry for entry in data.get("TabletStateInfo", [])
            if int(entry["TabletId"]) == tablet and entry.get("State") == "Active"
            and int(entry.get("FollowerId", 0)) == 0]
    if len(live) == 1:
        return {"node": int(live[0]["NodeId"]), "generation": int(live[0].get("Generation", 0))}
    return None


def owning_hive(destination):
    domain = destination.describe(destination.database_path)["DomainDescription"]
    hive = int(domain.get("ProcessingParams", {}).get("Hive") or domain.get("SharedHive") or 0)
    assert hive, "Tenant owning Hive is missing from its description"
    return hive


def tablet_action(destination, tablet, action):
    result = monitor(destination.cluster.nodes[1], "/tablets/app",
                     {"TabletID": owning_hive(destination), "page": action, "tablet": tablet}, method="post")
    assert result.get("Status") in ("OK", "ALREADY", 0, 2), result
    destination.record(action, tablet=tablet, result=result)


def move_controller(destination, tablet, process):
    node_id = actual_node_id(destination, process)
    hive = owning_hive(destination)
    result = monitor(destination.cluster.nodes[1], "/tablets/app",
                     {"TabletID": hive, "page": "MoveTablet", "tablet": tablet, "node": node_id},
                     method="post", timeout=RPC_TIMEOUT)
    assert result.get("status"), result
    def placed(timeout):
        location = controller_location(destination, tablet, timeout)
        return location if location and location["node"] == node_id else None
    location = destination.poll("controller relocated to requested process", placed)
    destination.record("controller-placement", tablet=tablet, location=location, binary=process.binary_path)
    return location


def process_lines(destination, process, offsets):
    paths = {process.stdout_file_name, process.stderr_file_name, process.ydbd_log_file_path}
    return [line for path, line in destination.log_lines_after(offsets) if path in paths]


def field(line, key):
    match = re.search(rf'\b{key}=("[^"]+"|\S+)', line)
    assert match, f"Missing structured {key} field"
    return match[1].strip('"')


def wait_worker(destination, process, offsets, expected=None):
    node_id = actual_node_id(destination, process)
    def ready(timeout):
        assert destination.replication(timeout).WhichOneof("state") != "error"
        lines = process_lines(destination, process, offsets)
        runs = [line for line in lines if "Run worker" in line and "actorClassName=TReplicationService" in line]
        actors = {field(line, "selfId") for line in lines
                  if "actorClassName=Worker " in line and "Handshake with reader" in line}
        if not runs or not actors:
            return None
        assert len(runs) == 1 and len(actors) == 1, "Isolated worker unexpectedly replaced or multiplied"
        logical = field(runs[0], "worker")
        assert expected is None or logical == expected, "Cold worker identity changed"
        worker = {"logical": logical, "actor": actors.pop(), "service": field(runs[0], "selfId"),
                  "node": node_id, "binary": process.binary_path}
        destination.record("worker-placement", worker=worker)
        return worker
    return destination.poll("isolated worker registration and handshake", ready)


def has_identity(line, logical):
    return all(re.search(rf"\b{name}:\s*{number}\b", line)
               for name, number in zip(("ReplicationId", "TargetId", "WorkerId"), logical.split(":")))


def wait_adoption(destination, controller, worker_process, worker, tablet, location, offsets):
    def adopted(timeout):
        service_lines = process_lines(destination, worker_process, offsets)
        assert not any("Run worker" in line or "Stop worker" in line or "Worker has gone" in line
                       for line in service_lines), "Controller move replaced the established worker"
        handshakes = [line for line in service_lines if "TEvHandshake" in line
                      and re.search(rf"TabletId:\s*{tablet}\b", line)
                      and re.search(rf"Generation:\s*{location['generation']}\b", line)]
        statuses = [line for line in process_lines(destination, controller, offsets)
                    if "TEvStatus" in line and "Workers" in line and has_identity(line, worker["logical"])]
        if not handshakes or not statuses:
            return False
        # Receipt is paired with a public request after the synchronous handler,
        # unchanged incarnation, and the controller's independently checked host.
        assert destination.replication(timeout).WhichOneof("state") != "error"
        assert controller_location(destination, tablet, timeout) == location
        destination.record("controller-adopted-worker", location=location, worker=worker,
                           handshake=destination.redact(handshakes[-1]), status=destination.redact(statuses[-1]))
        return True
    destination.poll("new generation adopts unchanged worker", adopted)


def wait_service_auth_error(destination, controller, service, worker, offsets):
    def observed(timeout):
        service_lines = process_lines(destination, service, offsets)
        auth = [line for line in service_lines if "Source authentication failed" in line
                and ("CLIENT_UNAUTHENTICATED" in line or "UNAUTHORIZED" in line)]
        status = [line for line in process_lines(destination, controller, offsets)
                  if "TEvWorkerStatus" in line and "STATUS_STOPPED" in line and "REASON_ERROR" in line
                  and has_identity(line, worker["logical"])]
        if auth and status:
            destination.record("mixed-service-auth-error", worker=worker,
                               source_auth=destination.redact(auth[-1]), controller_status=destination.redact(status[-1]))
            return True
        return False
    destination.poll("current service authentication error reaches old controller", observed)
