import logging
import os
import subprocess
import time
from contextlib import closing
from dataclasses import dataclass

import grpc
import requests
from google.protobuf.json_format import ParseDict

from ydb.core.protos import blobstorage_base3_pb2 as bs
from ydb.core.protos import blobstorage_distributed_config_pb2 as distconf
from ydb.core.protos import msgbus_pb2
from ydb.tests.library.clients.kikimr_client import KiKiMRMessageBusClient
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

logger = logging.getLogger(__name__)
ZONES = ("zone-0", "zone-1", "zone-2")


@dataclass(frozen=True)
class Group:
    generation: int
    state: str
    roles: dict
    health: str = ""

    def matches(self, expected):
        return (self.state, tuple(self.roles.get(dc) for dc in ZONES)) == expected


def excluded_zones(policy):
    return sorted(dc.Name for dc in policy.DataCenter if dc.State == bs.TDataCenterState.EXCLUDED)


def read_distconf(node):
    response = requests.post(
        f"http://{node.host}:{node.mon_port}/actors/nodewarden?page=distconf", json={"QueryConfig": {}}, timeout=4
    )
    response.raise_for_status()
    result = ParseDict(response.json(), distconf.TEvNodeConfigInvokeOnRootResult())
    if not result.HasField("QueryConfig"):
        raise RuntimeError(f"Distconf did not return a configuration: {result.ErrorReason}")
    config = result.QueryConfig.Config
    static = next((group for group in config.BlobStorageConfig.ServiceSet.Groups if group.GroupID == 0), None)
    if static is None:
        raise RuntimeError("Static group 0 is missing from the distconf configuration")

    locations = {node.NodeId: node.Location.DataCenter for node in config.AllNodes}
    roles = {}
    for ring in static.Rings:
        dcs = {locations.get(disk.NodeID) for domain in ring.FailDomains for disk in domain.VDiskLocations}
        if len(dcs) != 1 or None in dcs:
            raise RuntimeError(f"Unexpected static realm locations: {dcs}")
        roles[dcs.pop()] = bs.TDataCenterState.EState.Name(ring.State)
    return excluded_zones(config.DataCenterStates), Group(
        static.GroupGeneration, bs.TGroupState.EState.Name(static.Mirror3dcState), roles
    )


def read_bsc(client):
    request = msgbus_pb2.TBlobStorageConfigRequest(Domain=1)
    request.Request.Command.add().QueryBaseConfig.SetInParent()
    response = client.send(request, "BlobStorageConfig").BlobStorageConfigResponse
    if not response.Success or not response.Status or not response.Status[0].Success:
        raise RuntimeError(f"QueryBaseConfig failed: {response}")
    groups = {}
    for group in response.Status[0].BaseConfig.Group:
        if group.ErasureSpecies != "mirror-3-dc":
            continue
        roles = {r.DataCenter: bs.TDataCenterState.EState.Name(r.State) for r in group.FailRealms}
        groups[group.GroupId] = Group(
            group.GroupGeneration,
            bs.TGroupState.EState.Name(group.Mirror3dcState) if group.GroupId else "",
            roles,
            bs.TGroupStatus.E.Name(group.OperatingStatus),
        )
    return excluded_zones(response.AppliedDataCenterStates), groups


def observe(cluster, client, pids, excluded, static, dynamic, previous=None, timeout=90):
    deadline = time.monotonic() + timeout
    last = None
    while True:
        try:
            root = read_distconf(cluster.nodes[1])
        except (requests.RequestException, RuntimeError, ValueError) as error:
            root = str(error)
        try:
            bsc = read_bsc(client)
        except (grpc.RpcError, RuntimeError) as error:
            bsc = str(error)

        restarted = [i for i, node in cluster.nodes.items() if not node.is_alive() or node.pid != pids[i]]
        observed = f"distconf (excluded DCs, group 0)={root}, BSC (excluded DCs, groups)={bsc}, lost nodes={restarted}"
        if last != observed:
            logger.info("%s", observed)
            last = observed
        if not restarted and isinstance(root, tuple) and isinstance(bsc, tuple):
            root_policy, group_zero = root
            bsc_policy, groups = bsc
            if (
                root_policy == bsc_policy == sorted(excluded)
                and group_zero.matches(static)
                and 0 in groups
                and groups[0].roles == group_zero.roles
                and groups[0].generation == group_zero.generation
                and len(groups) >= 2
                and all(g.matches(dynamic) for gid, g in groups.items() if gid != 0)
                and (excluded or all(g.health == "FULL" for g in groups.values()))
                and (
                    previous is None
                    or (
                        groups.keys() == previous.keys()
                        and all(g.generation > previous[gid].generation for gid, g in groups.items())
                    )
                )
            ):
                return groups
        if restarted or time.monotonic() >= deadline:
            raise AssertionError(
                f"Expected excluded DCs {excluded}, static state {static}, dynamic state {dynamic}. "
                f"Last observation: {observed}"
            )
        time.sleep(1)


def test_takedown_and_rejoin_two_datacenters():
    configurator = KikimrConfigGenerator(
        Erasure.MIRROR_3_DC,
        nodes=9,
        simple_config=True,
        use_self_management=True,
        use_config_store=True,
        separate_node_configs=True,
        use_in_memory_pdisks=True,
        dynamic_storage_pools=[dict(name="dc-test", kind="hdd", pdisk_user_kind=0, num_groups=1)],
        extra_grpc_services=["config", "distributed_storage"],
    )
    for i, host in enumerate(configurator.yaml_config["hosts"], start=1):
        host["location"]["rack"] = f"rack-{i}"
    cluster = KiKiMR(configurator=configurator)
    try:
        cluster.start()
        pids = {i: node.pid for i, node in cluster.nodes.items()}
        node = cluster.nodes[1]
        endpoint = f"grpc://{node.host}:{node.grpc_port}"

        def cli(*args, timeout=30):
            command = [configurator.get_ydb_cli_path(), "-e", endpoint, "-d", "/Root", "-y", *args]
            result = subprocess.run(
                command,
                env=dict(os.environ, YDB_TOKEN="root@builtin"),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            assert result.returncode == 0, f"{args}: exit {result.returncode}: {result.stdout} {result.stderr}"
            return result.stdout

        i, x = "INCLUDED", "EXCLUDED"
        normal = ("NORMAL", (i, i, i))
        first_excluded = (x, i, i)
        both_excluded = (x, x, i)
        # Bound each observation even when BSC is unavailable.
        with closing(KiKiMRMessageBusClient(node.host, node.grpc_port, retry_count=1, timeout=4)) as client:
            groups = observe(cluster, client, pids, [], normal, normal)
            cli("sql", "-s", "CREATE TABLE dc_marker (id Uint64, value Utf8, PRIMARY KEY(id));")
            cli("sql", "-s", "UPSERT INTO dc_marker (id, value) VALUES (1u, CAST('before-takedown' AS Utf8));")
            read = ("sql", "-s", "SELECT value FROM dc_marker WHERE id = 1u;", "--format", "json-unicode")
            assert "before-takedown" in cli(*read)
            a, b, _ = ZONES
            for action, dc, excluded, static, dynamic in [
                ("takedown", a, [a], ("DC_FAILURE_INV2", first_excluded), ("DC_FAILURE_INV1", first_excluded)),
                ("takedown", b, [a, b], ("DC_SOLO_INV2", both_excluded), ("DC_SOLO_INV1", both_excluded)),
                ("rejoin", b, [a], ("DC_FAILURE_INV2", first_excluded), ("DC_FAILURE_INV1", first_excluded)),
                ("rejoin", a, [], normal, normal),
            ]:
                cli("admin", "storage", "datacenter", action, "--datacenter-name", dc)
                groups = observe(cluster, client, pids, excluded, static, dynamic, previous=groups)

            assert "before-takedown" in cli(*read)
    finally:
        cluster.stop()
