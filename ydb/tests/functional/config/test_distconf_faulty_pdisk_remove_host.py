# -*- coding: utf-8 -*-
import time

import pytest
import yaml
from hamcrest import assert_that

from ydb.core.protos import blobstorage_config_pb2 as bsconfig
import ydb.public.api.protos.ydb_config_pb2 as config_pb
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


@pytest.fixture(scope="function")
def cluster():
    log_configs = {
        'BS_NODE': LogLevels.DEBUG,
        'BS_CONTROLLER': LogLevels.DEBUG,
        'BS_SELFHEAL': LogLevels.DEBUG,
    }

    configurator = KikimrConfigGenerator(
        Erasure.BLOCK_4_2,
        nodes=9,
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
        extra_grpc_services=['config'],
        additional_log_configs=log_configs,
    )

    cluster = KiKiMR(configurator=configurator)
    cluster.start()
    cms.request_increase_ratio_limit(cluster.client)

    yield cluster

    cluster.stop()


def set_pdisk_faulty(cluster, node, pdisk_path):
    for attempt in range(10):
        resp = cluster.client.update_drive_status(
            node.host, node.ic_port, pdisk_path, bsconfig.EDriveStatus.FAULTY
        ).BlobStorageConfigResponse

        if resp.Success:
            return

        if (len(resp.Status) == 1 and resp.Status[0].FailReason == bsconfig.TConfigResponse.TStatus.EFailReason.kMayLoseData):
            time.sleep(10)
            continue

        raise RuntimeError("Failed to set PDisk FAULTY: %s" % resp)

    raise RuntimeError("Failed to set PDisk FAULTY after 10 attempts")


def wait_vslots_evacuated(cluster, node_id, pdisk_id, timeout=180):
    deadline = time.time() + timeout

    while time.time() < deadline:
        bc = cluster.client.query_base_config()
        vslots = [v for v in bc.BaseConfig.VSlot if v.VSlotId.NodeId == node_id and v.VSlotId.PDiskId == pdisk_id]

        if not vslots:
            return

        time.sleep(5)

    raise TimeoutError("VSlots not evacuated from [%d:%d] in %ds" % (node_id, pdisk_id, timeout))


def get_pdisk_info(cluster, node):
    swagger = SwaggerClient(node.host, node.mon_port)
    info = swagger.pdisk_info(node.node_id)
    pdisks = info.get("PDiskStateInfo", [])
    assert_that(len(pdisks) > 0, "No PDisk found on node %d" % node.node_id)
    return pdisks[0]["Path"], int(pdisks[0]["PDiskId"])


def fetch_config(cluster):
    resp = cluster.config_client.fetch_all_configs()
    assert_that(resp.operation.status == StatusIds.SUCCESS)
    result = config_pb.FetchConfigResult()
    resp.operation.result.Unpack(result)
    return yaml.safe_load(result.config[0].config)


def remove_node_from_config(config_dict, node):
    hosts = config_dict["config"]["hosts"]

    removed_hc_id = None
    for i, h in enumerate(hosts):
        if h.get("host") == node.host and h.get("port", node.port) == node.ic_port:
            removed_hc_id = h["host_config_id"]
            hosts.pop(i)
            break

    assert_that(removed_hc_id is not None, "Host not found in config")

    if not any(h["host_config_id"] == removed_hc_id for h in hosts):
        config_dict["config"]["host_configs"] = [
            hc for hc in config_dict["config"]["host_configs"] if hc["host_config_id"] != removed_hc_id
        ]


def apply_config(cluster, config_dict, retry_on_vslot_error=False, max_retries=30):
    for attempt in range(max_retries if retry_on_vslot_error else 1):
        current_config = fetch_config(cluster)
        config_dict["metadata"]["version"] = current_config["metadata"].get("version", 0) + 1

        resp = cluster.config_client.replace_config(yaml.dump(config_dict))

        if resp.operation.status == StatusIds.SUCCESS:
            return

        if retry_on_vslot_error and "has active VSlots" in str(resp.operation.issues):
            time.sleep(2)
            continue

        assert_that(False, "ReplaceConfig failed: %s" % resp.operation.issues)

    assert_that(False, "ReplaceConfig failed after %d retries" % max_retries)


def verify_node_removed(cluster, node):
    config = fetch_config(cluster)
    for h in config["config"]["hosts"]:
        if h.get("host") == node.host and h.get("port", node.port) == node.ic_port:
            raise AssertionError("Node still in config after removal")


class TestStaticGroupSelfHealWithNodeRemoval:

    def test_stop_node_then_faulty_then_remove(self, cluster):
        victim = cluster.nodes[max(cluster.nodes.keys())]
        pdisk_path, pdisk_id = get_pdisk_info(cluster, victim)

        victim.stop()
        set_pdisk_faulty(cluster, victim, pdisk_path)
        wait_vslots_evacuated(cluster, victim.node_id, pdisk_id)

        config = fetch_config(cluster)
        remove_node_from_config(config, victim)
        apply_config(cluster, config, retry_on_vslot_error=True)

        verify_node_removed(cluster, victim)

    def test_faulty_then_stop_then_remove(self, cluster):
        victim = cluster.nodes[max(cluster.nodes.keys())]
        pdisk_path, pdisk_id = get_pdisk_info(cluster, victim)

        set_pdisk_faulty(cluster, victim, pdisk_path)
        wait_vslots_evacuated(cluster, victim.node_id, pdisk_id)
        victim.stop()

        config = fetch_config(cluster)
        remove_node_from_config(config, victim)
        apply_config(cluster, config, retry_on_vslot_error=True)

        verify_node_removed(cluster, victim)
