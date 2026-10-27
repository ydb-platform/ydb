#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pytest

from ydb.core.protos import msgbus_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start


MIB = 1 << 20
DATA_SIZE = 512 * MIB
VALUE_SIZE = 4 * MIB

LOW_RATE = 2 * MIB
HIGH_RATE = 16 * MIB
LOW_RATE_WINDOW_SECONDS = 5
HIGH_RATE_WINDOW_SECONDS = 2

REPL_RATE_CONTROL = "VDiskControls.ReplTotalRequestBytesPerSecond"
REPL_BYTES_COUNTER = "ReplVGetBytesReceived"

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def cluster():
    configurator = KikimrConfigGenerator(
        erasure=Erasure.BLOCK_4_2,
        nodes=9,
        tiny_mode=True,
        dynamic_storage_pools=[{
            "name": "dynamic_storage_pool:1",
            "kind": "hdd",
            "pdisk_user_kind": 0,
            "num_groups": 1,
        }],
    )
    instance = KiKiMR(configurator=configurator)
    instance.start()
    try:
        yield instance
    finally:
        instance.stop()


def query_base_config(cluster):
    request = msgbus_pb2.TBlobStorageConfigRequest(Domain=1)
    request.Request.Rollback = True
    request.Request.Command.add().QueryBaseConfig.SetInParent()

    response = cluster.client.send(request, "BlobStorageConfig")
    assert response.Status == 1, response
    config_response = response.BlobStorageConfigResponse
    assert len(config_response.Status) == 1, config_response
    assert config_response.Status[0].Success, config_response.Status[0]
    return config_response.Status[0].BaseConfig


def wait_for_vdisk(cluster, predicate, description, timeout=240):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for vdisk in query_base_config(cluster).VSlot:
            if predicate(vdisk):
                return vdisk
        time.sleep(1)
    raise TimeoutError("Dynamic VDisk did not become %s within %s seconds" % (description, timeout))


def get_initial_vdisks(cluster, timeout=240):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        base_config = query_base_config(cluster)
        groups = [
            group for group in base_config.Group
            if (
                group.BoxId == 1
                and group.StoragePoolId == 1
                and group.OperatingStatus == 1
                and not group.HasField("VirtualGroupInfo")
            )
        ]
        populated_groups = []
        for group in groups:
            vdisks = [item for item in base_config.VSlot if item.GroupId == group.GroupId]
            allocated_size = sum(item.AllocatedSize for item in vdisks)
            if len(vdisks) == Erasure.BLOCK_4_2.min_fail_domains and allocated_size:
                populated_groups.append((allocated_size, vdisks))
        if populated_groups:
            return max(populated_groups, key=lambda item: item[0])[1]
        time.sleep(1)
    raise TimeoutError("Dynamic group did not become ready within %s seconds" % timeout)


def find_target_pdisk(cluster, group_vdisks):
    occupied_nodes = {vdisk.VSlotId.NodeId for vdisk in group_vdisks}
    base_config = query_base_config(cluster)
    candidates = sorted(
        (pdisk.NodeId, pdisk.PDiskId)
        for pdisk in base_config.PDisk
        if pdisk.NodeId not in occupied_nodes
    )
    assert candidates, "No spare PDisk outside the dynamic group"
    return candidates[0]


def reassign_vdisk(cluster, vdisk, target_pdisk):
    request = msgbus_pb2.TBlobStorageConfigRequest(Domain=1)
    command = request.Request.Command.add().ReassignGroupDisk
    command.GroupId = vdisk.GroupId
    command.GroupGeneration = vdisk.GroupGeneration
    command.FailRealmIdx = vdisk.FailRealmIdx
    command.FailDomainIdx = vdisk.FailDomainIdx
    command.VDiskIdx = vdisk.VDiskIdx
    command.TargetPDiskId.NodeId = target_pdisk[0]
    command.TargetPDiskId.PDiskId = target_pdisk[1]

    response = cluster.client.send(request, "BlobStorageConfig")
    assert response.Status == 1, response
    config_response = response.BlobStorageConfigResponse
    logger.info(
        "Reassigning group %s generation %s position %s:%s:%s from VSlot %s to PDisk %s; response: %s",
        vdisk.GroupId,
        vdisk.GroupGeneration,
        vdisk.FailRealmIdx,
        vdisk.FailDomainIdx,
        vdisk.VDiskIdx,
        vdisk.VSlotId,
        target_pdisk,
        config_response,
    )
    assert len(config_response.Status) == 1, config_response
    assert config_response.Status[0].Success, config_response.Status[0]
    assert config_response.Success, config_response


def set_icb(node, name, value):
    url = "http://{}:{}/actors/icb".format(node.host, node.mon_port)
    data = urlencode({name: value}).encode("ascii")
    with urlopen(Request(url, data=data, method="POST"), timeout=30) as response:
        assert response.status == 200
        body = response.read().decode("utf-8")
    expected_input = "name='{}' type='text' value='{}'".format(name, value)
    assert expected_input in body, "ICB did not apply {}={}".format(name, value)


def read_counter(node, sensor_name):
    url = "http://{}:{}/counters/json".format(node.host, node.mon_port)
    with urlopen(url, timeout=30) as response:
        counters = json.load(response)
    return sum(
        sensor.get("value", 0)
        for sensor in counters.get("sensors", [])
        if sensor.get("labels", {}).get("sensor") == sensor_name
    )


def read_repl_counters(node):
    url = "http://{}:{}/counters/json".format(node.host, node.mon_port)
    with urlopen(url, timeout=30) as response:
        counters = json.load(response)
    return {
        sensor["labels"]["sensor"]: sensor.get("value")
        for sensor in counters.get("sensors", [])
        if sensor.get("labels", {}).get("sensor", "").startswith("Repl")
    }


def wait_for_counter_increase(node, sensor_name, initial_value, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = read_counter(node, sensor_name)
        if value > initial_value:
            return value
        time.sleep(0.5)
    raise TimeoutError(
        "Counter %s did not increase within %s seconds; replication counters: %s"
        % (sensor_name, timeout, read_repl_counters(node))
    )


def measure_rate(node, sensor_name, duration):
    started_at = time.monotonic()
    initial_value = read_counter(node, sensor_name)
    time.sleep(duration)
    final_value = read_counter(node, sensor_name)
    elapsed = time.monotonic() - started_at
    assert final_value > initial_value, "%s did not increase during measurement" % sensor_name
    return (final_value - initial_value) / elapsed


def wait_for_rate_above(node, sensor_name, threshold, timeout=30):
    deadline = time.monotonic() + timeout
    previous_time = time.monotonic()
    previous_value = read_counter(node, sensor_name)
    while time.monotonic() < deadline:
        time.sleep(1)
        current_time = time.monotonic()
        current_value = read_counter(node, sensor_name)
        rate = (current_value - previous_value) / (current_time - previous_time)
        if rate >= threshold:
            return rate
        previous_time = current_time
        previous_value = current_value
    raise TimeoutError(
        "Counter %s did not reach %.2f MiB/s within %s seconds"
        % (sensor_name, threshold / MIB, timeout)
    )


def write_test_data(cluster, table_path):
    node = cluster.nodes[1]
    swagger = SwaggerClient(node.host, node.mon_port)
    create_kv_tablets_and_wait_for_start(
        cluster.client,
        cluster.kv_client,
        swagger,
        number_of_tablets=1,
        path=table_path,
        timeout_seconds=120,
    )

    value = os.urandom(VALUE_SIZE)
    keys = ["key-%06d" % index for index in range(DATA_SIZE // VALUE_SIZE)]
    for key in keys:
        response = cluster.kv_client.kv_write(
            table_path,
            partition_id=0,
            key=key,
            value=value,
        )
        assert response.operation.status == StatusIds.SUCCESS, response
    return value, keys


def test_replication_rate_control_is_applied_at_runtime(cluster):
    table_path = "/Root/replication-rate-control"
    value, keys = write_test_data(cluster, table_path)

    group_vdisks = get_initial_vdisks(cluster)
    source_vdisk = max(group_vdisks, key=lambda item: item.AllocatedSize)
    target_pdisk = find_target_pdisk(cluster, group_vdisks)
    target_node = cluster.nodes[target_pdisk[0]]

    set_icb(target_node, REPL_RATE_CONTROL, LOW_RATE)
    initial_counter = read_counter(target_node, REPL_BYTES_COUNTER)
    reassign_vdisk(cluster, source_vdisk, target_pdisk)

    wait_for_counter_increase(target_node, REPL_BYTES_COUNTER, initial_counter)

    low_rate = measure_rate(target_node, REPL_BYTES_COUNTER, LOW_RATE_WINDOW_SECONDS)
    assert low_rate <= LOW_RATE * 1.5, (
        "Low replication rate %.2f MiB/s exceeds the configured %.2f MiB/s"
        % (low_rate / MIB, LOW_RATE / MIB)
    )

    set_icb(target_node, REPL_RATE_CONTROL, HIGH_RATE)
    wait_for_rate_above(target_node, REPL_BYTES_COUNTER, low_rate * 2.5)
    high_rate = measure_rate(target_node, REPL_BYTES_COUNTER, HIGH_RATE_WINDOW_SECONDS)
    logger.info(
        "Measured replication rates: low %.2f MiB/s, high %.2f MiB/s",
        low_rate / MIB,
        high_rate / MIB,
    )
    assert high_rate >= low_rate * 2.5, (
        "Replication rate did not increase enough after the runtime ICB update: "
        "low %.2f MiB/s, high %.2f MiB/s"
        % (low_rate / MIB, high_rate / MIB)
    )

    wait_for_vdisk(
        cluster,
        lambda item: (
            item.GroupId == source_vdisk.GroupId
            and (item.VSlotId.NodeId, item.VSlotId.PDiskId) == target_pdisk
            and item.Ready
        ),
        "fully replicated and ready",
    )

    for key in (keys[0], keys[-1]):
        response = cluster.kv_client.kv_read(table_path, partition_id=0, key=key)
        assert response.operation.status == StatusIds.SUCCESS, response
        assert value in response.operation.result.value
