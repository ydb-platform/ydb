# -*- coding: utf-8 -*-
import time

import pytest
import yaml
from hamcrest import assert_that

from ydb.core.protos import blobstorage_config_pb2 as bsconfig
import ydb.public.api.protos.ydb_config_pb2 as config_pb
from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


STATIC_GROUP_ID = 0


@pytest.fixture(scope="function")
def cluster():
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
        extra_grpc_services=['config'],
        additional_log_configs=log_configs,
    )

    cluster = KiKiMR(configurator=configurator)
    cluster.start()
    cms.request_increase_ratio_limit(cluster.client)

    yield cluster

    cluster.stop()


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
            node.host, node.ic_port, path, bsconfig.EDriveStatus.FAULTY
        ).BlobStorageConfigResponse

        if resp.Success:
            return

        if (len(resp.Status) == 1
                and resp.Status[0].FailReason == bsconfig.TConfigResponse.TStatus.EFailReason.kMayLoseData):
            time.sleep(10)
            continue

        raise RuntimeError("Failed to set PDisk FAULTY: %s" % resp)

    raise RuntimeError("Failed to set PDisk FAULTY after 10 attempts")


def wait_static_group_relocated(cluster, victim_node_id, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        nodes = static_group_nodes(cluster)
        if victim_node_id not in nodes:
            return nodes
        time.sleep(5)
    raise TimeoutError("Static group vdisk not relocated off node %d in %ds" % (victim_node_id, timeout))


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
