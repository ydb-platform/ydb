# -*- coding: utf-8 -*-
import time

import grpc
import pytest
import yaml
from hamcrest import assert_that

from ydb.core.protos import blobstorage_base3_pb2 as bsbase
from ydb.core.protos import blobstorage_config_pb2 as bsconfig
from ydb.core.protos import msgbus_pb2
from ydb.public.api.grpc.draft import ydb_distributed_storage_v1_pb2_grpc as distributed_storage_grpc
from ydb.public.api.protos.draft import ydb_distributed_storage_pb2 as distributed_storage
import ydb.public.api.protos.ydb_config_pb2 as config_pb
from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


STATIC_GROUP_ID = 0


def create_cluster(dynamic_pdisks=None):
    log_configs = {
        'BS_NODE': LogLevels.DEBUG,
        'BS_CONTROLLER': LogLevels.DEBUG,
        'BS_SELFHEAL': LogLevels.DEBUG,
    }

    configurator = KikimrConfigGenerator(
        Erasure.BLOCK_4_2,
        nodes=10,
        use_in_memory_pdisks=False,
        use_config_store=True,
        metadata_section={
            "kind": "MainConfig",
            "version": 0,
            "cluster": ""
        },
        separate_node_configs=True,
        simple_config=True,
        use_self_management=True,
        dynamic_pdisks=dynamic_pdisks or [],
        extra_grpc_services=['config', 'distributed_storage'],
        additional_log_configs=log_configs,
    )

    cluster = KiKiMR(configurator=configurator)
    cluster.start()
    cms.request_increase_ratio_limit(cluster.client)
    return cluster


def running_cluster(dynamic_pdisks=None):
    cluster = create_cluster(dynamic_pdisks)

    try:
        yield cluster
    finally:
        cluster.stop()


@pytest.fixture(scope="function")
def cluster():
    yield from running_cluster()


@pytest.fixture(scope="function")
def cluster_with_local_spare_pdisk():
    yield from running_cluster([{}])


def fetch_config(cluster):
    resp = cluster.config_client.fetch_all_configs()
    assert_that(resp.operation.status == StatusIds.SUCCESS)
    result = config_pb.FetchConfigResult()
    resp.operation.result.Unpack(result)
    return yaml.safe_load(result.config[0].config)


def apply_config(cluster, config_dict):
    current_config = fetch_config(cluster)
    config_dict["metadata"]["version"] = current_config["metadata"].get("version", 0) + 1
    resp = cluster.config_client.replace_config(yaml.dump(config_dict))
    assert_that(resp.operation.status == StatusIds.SUCCESS, "ReplaceConfig failed: %s" % resp.operation.issues)


def static_group_nodes(cluster):
    bc = cluster.client.query_base_config()
    return {v.VSlotId.NodeId for v in bc.BaseConfig.VSlot if v.GroupId == STATIC_GROUP_ID}


def static_group_vslot_on_node(cluster, node_id):
    bc = cluster.client.query_base_config()
    for v in bc.BaseConfig.VSlot:
        if v.GroupId == STATIC_GROUP_ID and v.VSlotId.NodeId == node_id:
            return v
    return None


def pdisk_path(cluster, node_id, pdisk_id):
    bc = cluster.client.query_base_config()
    for p in bc.BaseConfig.PDisk:
        if p.NodeId == node_id and p.PDiskId == pdisk_id:
            return p.Path
    raise RuntimeError("PDisk [%d:%d] not found" % (node_id, pdisk_id))


def set_pdisk_faulty(cluster, node, path):
    for attempt in range(10):
        resp = cluster.client.update_drive_status(
            node.host, node.ic_port, path, bsbase.EDriveStatus.FAULTY
        ).BlobStorageConfigResponse

        if resp.Success:
            return

        if (len(resp.Status) == 1
                and resp.Status[0].FailReason == bsconfig.TConfigResponse.TStatus.EFailReason.kMayLoseData):
            time.sleep(10)
            continue

        raise RuntimeError("Failed to set PDisk FAULTY: %s" % resp)

    raise RuntimeError("Failed to set PDisk FAULTY after 10 attempts")


def set_use_self_heal_local_policy(cluster, value):
    request = msgbus_pb2.TBlobStorageConfigRequest()
    request.Domain = 1
    request.Request.Command.add().UpdateSettings.UseSelfHealLocalPolicy.append(value)
    response = cluster.client.send(request, 'BlobStorageConfig').BlobStorageConfigResponse
    assert_that(response.Success, "UpdateSettings failed: %s" % response)


def wait_static_group_relocated(cluster, victim_node_id, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        nodes = static_group_nodes(cluster)
        if victim_node_id not in nodes:
            return nodes
        time.sleep(5)
    raise TimeoutError("Static group vdisk not relocated off node %d in %ds" % (victim_node_id, timeout))


def vdisk_position(vdisk_id):
    return (
        vdisk_id.group_id,
        vdisk_id.fail_realm_idx,
        vdisk_id.fail_domain_idx,
        vdisk_id.vdisk_idx,
    )


def vslot_id(vslot):
    return vslot.node_id, vslot.pdisk_id, vslot.vslot_id


def stream_static_vdisks(stub):
    request = distributed_storage.StorageStateRequest(include_vdisks=True)
    vdisks = []
    for response in stub.StreamStorageState(request, timeout=30):
        assert_that(
            response.status == StatusIds.SUCCESS,
            "StreamStorageState failed: %s" % response,
        )
        if response.HasField("result"):
            for vdisk in response.result.vdisks:
                if vdisk.is_static:
                    copy = distributed_storage.VDisk()
                    copy.CopyFrom(vdisk)
                    vdisks.append(copy)
    return vdisks


def wait_static_vdisks_ready(stub, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        vdisks = stream_static_vdisks(stub)
        if vdisks and all(vdisk.ready for vdisk in vdisks):
            return vdisks
        time.sleep(2)
    raise TimeoutError("Static group VDisks did not become ready within %s seconds" % timeout)


def wait_static_vdisk_reassignment(stub, previous_vdisk, predicate, description, timeout=180):
    position = vdisk_position(previous_vdisk.id)
    deadline = time.time() + timeout
    while time.time() < deadline:
        for vdisk in stream_static_vdisks(stub):
            if (
                vdisk_position(vdisk.id) == position
                and vdisk.id.group_generation > previous_vdisk.id.group_generation
                and predicate(vdisk)
            ):
                return vdisk
        time.sleep(2)
    raise TimeoutError(
        "Static VDisk %s was not %s within %s seconds"
        % (position, description, timeout)
    )


def wait_static_vdisk_reassigned(stub, previous_vdisk, target_slot, timeout=180):
    target = vslot_id(target_slot)
    return wait_static_vdisk_reassignment(
        stub,
        previous_vdisk,
        lambda vdisk: vslot_id(vdisk.slot_id) == target,
        "reassigned to VSlot %s" % (target,),
        timeout,
    )


def wait_static_vdisk_self_healed(stub, previous_vdisk, timeout=180):
    source = vslot_id(previous_vdisk.slot_id)
    return wait_static_vdisk_reassignment(
        stub,
        previous_vdisk,
        lambda vdisk: vslot_id(vdisk.slot_id) != source,
        "self-healed away from VSlot %s" % (source,),
        timeout,
    )


def find_target_pdisk(cluster, vdisks):
    occupied_nodes = {vdisk.slot_id.node_id for vdisk in vdisks}
    base_config = cluster.client.query_base_config().BaseConfig
    candidates = [
        (pdisk.NodeId, pdisk.PDiskId)
        for pdisk in base_config.PDisk
        if (
            pdisk.DriveStatus == bsbase.EDriveStatus.ACTIVE
            and pdisk.NodeId not in occupied_nodes
        )
    ]
    assert_that(candidates, "No active target PDisk is available")
    return min(candidates)


def reassign_static_vdisk(stub, previous_vdisk, target_pdisk=None):
    request = distributed_storage.ReassignVDiskRequest()
    request.vdisk_id.CopyFrom(previous_vdisk.id)
    if target_pdisk is not None:
        request.target_pdisk_id.node_id = target_pdisk[0]
        request.target_pdisk_id.pdisk_id = target_pdisk[1]

    response = stub.ReassignVDisk(request, timeout=180)
    assert_that(response.operation.ready, "ReassignVDisk operation is not ready")
    assert_that(
        response.operation.status == StatusIds.SUCCESS,
        "ReassignVDisk failed: %s" % response.operation,
    )

    result = distributed_storage.ReassignVDiskResult()
    assert_that(response.operation.result.Unpack(result), "Unexpected ReassignVDisk result type")
    assert_that(result.vdisk_id == request.vdisk_id)
    assert_that(vslot_id(result.source_slot_id) == vslot_id(previous_vdisk.slot_id))
    assert_that(
        vslot_id(result.target_slot_id)[:2] != vslot_id(result.source_slot_id)[:2],
        "ReassignVDisk kept the static VDisk on the same PDisk",
    )
    if target_pdisk is not None:
        assert_that(
            vslot_id(result.target_slot_id)[:2] == target_pdisk,
            "ReassignVDisk ignored the explicit target PDisk",
        )

    return wait_static_vdisk_reassigned(stub, previous_vdisk, result.target_slot_id)


class TestStaticGroupManualReassign:

    def test_evict_static_vdisk(self, cluster):
        node = cluster.nodes[1]
        with grpc.insecure_channel("%s:%s" % (node.host, node.grpc_port)) as channel:
            stub = distributed_storage_grpc.DistributedStorageServiceStub(channel)
            vdisks = wait_static_vdisks_ready(stub)

            previous_vdisk = min(vdisks, key=lambda vdisk: vdisk_position(vdisk.id))
            reassign_static_vdisk(stub, previous_vdisk)

    def test_reassign_static_vdisk(self, cluster):
        node = cluster.nodes[1]
        with grpc.insecure_channel("%s:%s" % (node.host, node.grpc_port)) as channel:
            stub = distributed_storage_grpc.DistributedStorageServiceStub(channel)
            vdisks = wait_static_vdisks_ready(stub)

            previous_vdisk = min(vdisks, key=lambda vdisk: vdisk_position(vdisk.id))
            target_pdisk = find_target_pdisk(cluster, vdisks)
            reassign_static_vdisk(stub, previous_vdisk, target_pdisk)


class TestStaticGroupSelfHealAllowedNodes:

    def test_self_heal_relocates_only_to_allowed_nodes(self, cluster):
        group_nodes = static_group_nodes(cluster)
        all_nodes = set(cluster.nodes.keys())
        free_nodes = sorted(all_nodes - group_nodes)

        if len(free_nodes) < 2:
            pytest.skip("need at least 2 free nodes to make the allow-list meaningful, got %d" % len(free_nodes))

        allowed_node_id = free_nodes[0]
        forbidden_node_id = free_nodes[1]

        config = fetch_config(cluster)
        smc = config["config"].setdefault("self_management_config", {})
        smc["enabled"] = True
        smc["automatic_static_group_management"] = True
        smc["static_group_self_heal_allowed_nodes"] = [allowed_node_id]
        apply_config(cluster, config)

        victim_node_id = sorted(group_nodes)[0]
        victim = cluster.nodes[victim_node_id]
        vslot = static_group_vslot_on_node(cluster, victim_node_id)
        assert_that(vslot is not None, "no static group vslot on victim node")
        path = pdisk_path(cluster, victim_node_id, vslot.VSlotId.PDiskId)

        set_pdisk_faulty(cluster, victim, path)

        new_group_nodes = wait_static_group_relocated(cluster, victim_node_id)

        assert_that(allowed_node_id in new_group_nodes,
                    "static group vdisk did not move to the allowed node (node %d); group nodes: %s"
                    % (allowed_node_id, sorted(new_group_nodes)))
        assert_that(forbidden_node_id not in new_group_nodes,
                    "static group vdisk moved onto the forbidden node (node %d); group nodes: %s"
                    % (forbidden_node_id, sorted(new_group_nodes)))
        assert_that(victim_node_id not in new_group_nodes,
                    "static group vdisk still present on the faulty victim node %d" % victim_node_id)


class TestStaticGroupSelfHealPlacementPolicy:

    def test_local_policy_relocates_to_spare_pdisk_on_same_node(self, cluster_with_local_spare_pdisk):
        cluster = cluster_with_local_spare_pdisk
        config = fetch_config(cluster)
        smc = config["config"].setdefault("self_management_config", {})
        smc["enabled"] = True
        smc["automatic_static_group_management"] = True
        apply_config(cluster, config)
        set_use_self_heal_local_policy(cluster, True)

        node = cluster.nodes[1]
        with grpc.insecure_channel("%s:%s" % (node.host, node.grpc_port)) as channel:
            stub = distributed_storage_grpc.DistributedStorageServiceStub(channel)
            vdisks = wait_static_vdisks_ready(stub)
            previous_vdisk = min(vdisks, key=lambda vdisk: vdisk_position(vdisk.id))
            source = vslot_id(previous_vdisk.slot_id)

            base_config = cluster.client.query_base_config().BaseConfig
            local_spares = [
                pdisk.PDiskId
                for pdisk in base_config.PDisk
                if (
                    pdisk.NodeId == source[0]
                    and pdisk.PDiskId != source[1]
                    and pdisk.DriveStatus == bsbase.EDriveStatus.ACTIVE
                )
            ]
            assert_that(local_spares, "No local spare PDisk is available")

            victim = cluster.nodes[source[0]]
            path = pdisk_path(cluster, source[0], source[1])
            set_pdisk_faulty(cluster, victim, path)

            replacement = wait_static_vdisk_self_healed(stub, previous_vdisk)
            target = vslot_id(replacement.slot_id)
            assert_that(target[0] == source[0],
                        "local self-heal moved VDisk to another node: %s -> %s" % (source, target))
            assert_that(target[1] in local_spares,
                        "local self-heal did not use a spare PDisk: %s -> %s" % (source, target))
