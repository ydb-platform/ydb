#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import time
from urllib.request import urlopen

import grpc
import pytest
import yaml

from ydb.public.api.grpc.draft import ydb_distributed_storage_v1_pb2_grpc as distributed_storage_grpc
from ydb.public.api.protos.draft import ydb_dynamic_config_pb2 as dynamic_config
from ydb.public.api.protos.draft import ydb_distributed_storage_pb2 as distributed_storage
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.clients.kikimr_dynconfig_client import DynConfigClient
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start


MIB = 1 << 20
VALUE_SIZE = 512 << 10
VALUES_PER_REQUEST = 8
PARTITIONS = 8
DATA_PER_PARTITION = 512 * MIB
REPLICATION_RATE = 16 * MIB
REPLICATION_QUANTUM = 384 * MIB

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def cluster():
    configurator = KikimrConfigGenerator(
        erasure=Erasure.BLOCK_4_2,
        # Two 4+2 block groups occupy at most 16 nodes, so node 17 guarantees a PDisk outside both groups.
        nodes=17,
        tiny_mode=True,
        dynamic_storage_pools=[{
            "name": "dynamic_storage_pool:1",
            "kind": "hdd",
            "pdisk_user_kind": 0,
            "num_groups": 2,
        }],
        extra_grpc_services=["distributed_storage"],
        extra_feature_flags=["enable_repl_token_priority"],
    )
    instance = KiKiMR(configurator=configurator)
    instance.start()
    try:
        yield instance
    finally:
        instance.stop()


def stream_vdisks(stub):
    request = distributed_storage.StorageStateRequest(include_vdisks=True)
    vdisks = []
    for response in stub.StreamStorageState(request, timeout=30):
        assert response.status == StatusIds.SUCCESS, response
        if response.HasField("result"):
            for vdisk in response.result.vdisks:
                copy = distributed_storage.VDisk()
                copy.CopyFrom(vdisk)
                vdisks.append(copy)
    return [vdisk for vdisk in vdisks if not vdisk.is_static]


def wait_for_initial_vdisks(stub, timeout=240):
    deadline = time.monotonic() + timeout
    expected = 2 * Erasure.BLOCK_4_2.min_fail_domains
    while time.monotonic() < deadline:
        vdisks = stream_vdisks(stub)
        if len(vdisks) == expected and all(vdisk.ready for vdisk in vdisks):
            groups = {vdisk.id.group_id for vdisk in vdisks}
            if len(groups) == 2:
                return vdisks
        time.sleep(1)
    raise TimeoutError("Two dynamic groups did not become ready within %s seconds" % timeout)


def group_vdisks(vdisks):
    result = {}
    for vdisk in vdisks:
        result.setdefault(vdisk.id.group_id, []).append(vdisk)
    return result


def choose_sources_and_urgent_only_node(groups, target_node_id):
    # Calculate membership after both source slots have been moved to the common target. Trying all pairs also covers
    # identical initial placement: moving different source nodes makes each resulting group have an exclusive node.
    candidates = []
    for regular_id, regular in groups.items():
        for urgent_id, urgent in groups.items():
            if regular_id == urgent_id:
                continue
            for regular_source in sorted(regular, key=lambda item: item.allocated_size, reverse=True):
                regular_nodes = {vdisk.slot_id.node_id for vdisk in regular}
                regular_nodes.discard(regular_source.slot_id.node_id)
                regular_nodes.add(target_node_id)
                for urgent_source in sorted(urgent, key=lambda item: item.allocated_size, reverse=True):
                    urgent_nodes = {vdisk.slot_id.node_id for vdisk in urgent}
                    urgent_nodes.discard(urgent_source.slot_id.node_id)
                    urgent_nodes.add(target_node_id)
                    urgent_only_nodes = sorted(urgent_nodes - regular_nodes - {1, target_node_id})
                    if urgent_only_nodes:
                        candidates.append((regular_source, urgent_source, urgent_only_nodes[0]))
    assert candidates, "The reassigned groups need a non-control node exclusive to the urgent group"
    return max(candidates, key=lambda item: (item[0].allocated_size, item[1].allocated_size))


def find_shared_target_pdisk(cluster, groups):
    occupied_nodes = {
        vdisk.slot_id.node_id
        for vdisks in groups.values()
        for vdisk in vdisks
    }
    base_config = cluster.client.query_base_config().BaseConfig
    candidates = sorted(
        (pdisk.NodeId, pdisk.PDiskId)
        for pdisk in base_config.PDisk
        if pdisk.NodeId not in occupied_nodes and pdisk.NodeId != 1
    )
    assert candidates, "The test needs a spare PDisk outside both dynamic groups"
    return candidates[0]


def reassign_vdisk(stub, vdisk, target_pdisk):
    request = distributed_storage.ReassignVDiskRequest()
    request.vdisk_id.CopyFrom(vdisk.id)
    request.target_pdisk_id.node_id = target_pdisk[0]
    request.target_pdisk_id.pdisk_id = target_pdisk[1]
    response = stub.ReassignVDisk(request, timeout=180)
    assert response.operation.ready, response.operation
    assert response.operation.status == StatusIds.SUCCESS, response.operation
    result = distributed_storage.ReassignVDiskResult()
    assert response.operation.result.Unpack(result), response.operation.result
    assert (result.target_slot_id.node_id, result.target_slot_id.pdisk_id) == target_pdisk
    return result.target_slot_id


def make_dynamic_config(client):
    response = client.fetch_startup_config()
    assert response.operation.status == StatusIds.SUCCESS, response.operation
    result = dynamic_config.FetchStartupConfigResult()
    assert response.operation.result.Unpack(result), response.operation.result
    return {
        "metadata": {"kind": "MainConfig", "version": 0, "cluster": ""},
        "config": yaml.safe_load(result.config),
    }


def set_replication_rate(client, config, value):
    blob_storage_config = config["config"].setdefault("blob_storage_config", {})
    service_set = blob_storage_config.setdefault("service_set", {})
    repl_broker_config = service_set.setdefault("repl_broker_config", {})
    repl_broker_config["total_request_bytes_per_second"] = value
    response = client.replace_config(yaml.safe_dump(config))
    assert response.operation.status == StatusIds.SUCCESS, response.operation


def write_test_data(cluster, table_path):
    node = cluster.nodes[1]
    swagger = SwaggerClient(node.host, node.mon_port)
    create_kv_tablets_and_wait_for_start(
        cluster.client,
        cluster.kv_client,
        swagger,
        number_of_tablets=PARTITIONS,
        path=table_path,
        timeout_seconds=180,
    )
    value = os.urandom(VALUE_SIZE)
    keys_per_partition = DATA_PER_PARTITION // VALUE_SIZE
    for partition_id in range(PARTITIONS):
        keys = ["p%02d-key-%06d" % (partition_id, index) for index in range(keys_per_partition)]
        for offset in range(0, len(keys), VALUES_PER_REQUEST):
            response = cluster.kv_client.kv_writes(
                table_path,
                partition_id=partition_id,
                kv_pairs=[(key, value) for key in keys[offset:offset + VALUES_PER_REQUEST]],
                channel=2,
            )
            assert response.operation.status == StatusIds.SUCCESS, response
    return value


def read_vdisk_info(node):
    url = "http://{}:{}/viewer/vdiskinfo?fields_required=all&enums=true".format(
        node.host, node.mon_port
    )
    with urlopen(url, timeout=30) as response:
        return json.load(response).get("VDiskStateInfo", [])


def vdisk_position(vdisk):
    return (
        vdisk.id.group_id,
        vdisk.id.fail_realm_idx,
        vdisk.id.fail_domain_idx,
        vdisk.id.vdisk_idx,
    )


def find_vdisk(info, vdisk):
    expected = vdisk_position(vdisk)
    for item in info:
        vdisk_id = item.get("VDiskId", {})
        actual = (
            vdisk_id.get("GroupID"),
            vdisk_id.get("Ring", 0),
            vdisk_id.get("Domain", 0),
            vdisk_id.get("VDisk", 0),
        )
        if actual == expected:
            return item
    return None


def replication_status(vdisk):
    status = vdisk.get("DetailedReplicationStatus")
    return {
        0: "Replicated",
        1: "WaitingForToken",
        2: "InProgress",
        3: "PhantomsOnly",
    }.get(status, status)


def wait_for_target_states(node, regular_vdisk, urgent_vdisk, predicate, description, timeout=300):
    deadline = time.monotonic() + timeout
    last = None
    last_statuses = None
    while time.monotonic() < deadline:
        try:
            info = read_vdisk_info(node)
        except (OSError, TimeoutError, ConnectionError):
            time.sleep(1)
            continue
        regular = find_vdisk(info, regular_vdisk)
        urgent = find_vdisk(info, urgent_vdisk)
        last = regular, urgent
        if regular and urgent:
            statuses = (
                regular.get("Replicated", False),
                replication_status(regular),
                regular.get("ReplicationProgress"),
                urgent.get("Replicated", False),
                replication_status(urgent),
                urgent.get("ReplicationProgress"),
            )
            if statuses != last_statuses:
                logger.info("Replication states: regular=%s urgent=%s", statuses[:3], statuses[3:])
                last_statuses = statuses
        if regular and urgent and predicate(regular, urgent):
            logger.info("Reached %s: regular=%s urgent=%s", description, regular, urgent)
            return regular, urgent
        time.sleep(1)
    raise TimeoutError("Did not reach %s within %s seconds; last states: %s" % (description, timeout, last))


def wait_for_all_ready(stub, positions, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ready = {
            (vdisk.id.group_id, vdisk.id.fail_realm_idx, vdisk.id.fail_domain_idx, vdisk.id.vdisk_idx)
            for vdisk in stream_vdisks(stub)
            if vdisk.ready
        }
        if positions <= ready:
            return
        time.sleep(1)
    raise TimeoutError("Reassigned VDisks did not finish replication within %s seconds" % timeout)


def test_more_degraded_group_preempts_replication_on_shared_pdisk(cluster):
    # End-to-end contract: both real VDisk schedulers recover onto one PDisk and one group is additionally made -2.
    # Once the target process restarts, the -2 scheduler must hold the single PDisk token while the -1 scheduler reports
    # WaitingForToken. After the urgent VDisk completes, the regular VDisk may continue.
    api_node = cluster.nodes[1]
    dynconfig_client = DynConfigClient(api_node.host, api_node.grpc_port)
    try:
        config = make_dynamic_config(dynconfig_client)
        set_replication_rate(dynconfig_client, config, REPLICATION_RATE)
    finally:
        dynconfig_client.close()

    table_path = "/Root/replication-token-priority"
    value = write_test_data(cluster, table_path)

    endpoint = "{}:{}".format(api_node.host, api_node.grpc_port)
    with grpc.insecure_channel(endpoint) as channel:
        stub = distributed_storage_grpc.DistributedStorageServiceStub(channel)
        initial_vdisks = wait_for_initial_vdisks(stub)
        groups = group_vdisks(initial_vdisks)
        target_pdisk = find_shared_target_pdisk(cluster, groups)
        regular_source, urgent_source, urgent_only_node_id = choose_sources_and_urgent_only_node(
            groups, target_pdisk[0]
        )
        # Yield is cooperative at the 384 MiB production quantum boundary. More work than one quantum is required to
        # distinguish preemption from the regular VDisk simply finishing and releasing its token.
        assert regular_source.allocated_size >= REPLICATION_QUANTUM + 32 * MIB
        assert urgent_source.allocated_size >= 32 * MIB

        reassign_vdisk(stub, regular_source, target_pdisk)
        reassign_vdisk(stub, urgent_source, target_pdisk)
        target_node = cluster.nodes[target_pdisk[0]]
        urgent_only_node = cluster.nodes[urgent_only_node_id]
        target_stopped = False
        urgent_only_stopped = False
        try:
            # Restarting the common target makes both schedulers compete from a comparable startup point instead of
            # depending on the duration of the two sequential ReassignVDisk RPCs.
            target_node.stop()
            target_stopped = True
            urgent_only_node.stop()
            urgent_only_stopped = True
            target_node.start()
            target_stopped = False

            wait_for_target_states(
                target_node,
                regular_source,
                urgent_source,
                lambda regular, urgent: (
                    not regular.get("Replicated", False)
                    and replication_status(regular) == "WaitingForToken"
                    and not urgent.get("Replicated", False)
                    and replication_status(urgent) == "InProgress"
                ),
                "urgent replication owning the token while regular replication waits",
            )
            wait_for_target_states(
                target_node,
                regular_source,
                urgent_source,
                lambda regular, urgent: urgent.get("Replicated", False),
                "urgent replication complete",
            )
            wait_for_target_states(
                target_node,
                regular_source,
                urgent_source,
                lambda regular, urgent: replication_status(regular) == "InProgress"
                or regular.get("Replicated", False),
                "regular replication resumed after urgent replication",
            )
        finally:
            if target_stopped:
                target_node.start()
            if urgent_only_stopped:
                urgent_only_node.start()

        positions = {
            (
                source.id.group_id,
                source.id.fail_realm_idx,
                source.id.fail_domain_idx,
                source.id.vdisk_idx,
            )
            for source in (regular_source, urgent_source)
        }
        wait_for_all_ready(stub, positions)

    for partition_id in range(PARTITIONS):
        key = "p%02d-key-%06d" % (partition_id, 0)
        response = cluster.kv_client.kv_read(table_path, partition_id=partition_id, key=key)
        assert response.operation.status == StatusIds.SUCCESS, response
        assert value in response.operation.result.value
