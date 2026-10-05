from types import SimpleNamespace
from unittest import mock

import pytest

from ydb.core.protos import bootstrap_pb2
from ydb.tools.cfg import types
from ydb.tools.cfg.static import StaticConfigGenerator

# (name, non_fixed_tablet_types_id, fixed_tablet_types_id)
DEFAULT_TABLETS = (
    ("FLAT_HIVE", 0x010000000000A001, 0x010000000000A001),
    ("FLAT_BS_CONTROLLER", 0x0100000000001001, 0x0100000000001001),
    ("FLAT_SCHEMESHARD", 0x01001000008587A0, 0x01000000008587A0),
    ("FLAT_TX_COORDINATOR", 0x0100100000800001, 0x0100000000800001),
    ("TX_MEDIATOR", 0x0100100000810001, 0x0100000000810001),
    ("TX_ALLOCATOR", 0x0100100000820001, 0x0100000000820001),
    ("CMS", 0x0100000000002000, 0x0100000000002000),
    ("NODE_BROKER", 0x0100000000002001, 0x0100000000002001),
    ("TENANT_SLOT_BROKER", 0x0100000000002002, 0x0100000000002002),
    ("CONSOLE", 0x0100000000002003, 0x0100000000002003),
)


def expected_tablet(tablet_name, tablet_id):
    tablet = bootstrap_pb2.TBootstrap.TTablet(
        Type=bootstrap_pb2.TBootstrap.ETabletType.Value(tablet_name),
        Node=[1, 2],
    )
    tablet.Info.TabletID = tablet_id
    for channel_id in range(3):
        channel = tablet.Info.Channels.add(Channel=channel_id, ChannelErasureName="none")
        channel.History.add(FromGeneration=0, GroupID=0)
    return tablet


def expected_bootstrap(fixed_tablet_types):
    boot = bootstrap_pb2.TBootstrap()
    for name, non_fixed_tablet_types_id, fixed_tablet_types_id in DEFAULT_TABLETS:
        tablet_id = fixed_tablet_types_id if fixed_tablet_types else non_fixed_tablet_types_id
        boot.Tablet.add().CopyFrom(expected_tablet(name, tablet_id))
    return boot


def generate_bootstrap(fixed_tablet_types, system_tablets):
    # Only bootstrap generation is under test. Host discovery, validation and
    # storage-group selection belong to ClusterDetailsProvider.
    details = SimpleNamespace(
        use_k8s_api=False,
        use_fixed_tablet_types=fixed_tablet_types,
        domains=[SimpleNamespace(coordinators=1, mediators=1, allocators=1)],
        system_tablets_config=system_tablets,
        system_tablets_node_ids=[1, 2],
        static_erasure=types.Erasure.NONE,
        bootstrap_config=None,
        shared_cache_memory_limit=None,
        pq_shared_cache_size=None,
    )
    with mock.patch("ydb.tools.cfg.static.base.ClusterDetailsProvider", return_value=details):
        return StaticConfigGenerator(
            {"system_tablets": system_tablets}, binary_path="", output_dir=""
        ).boot_txt


@pytest.mark.parametrize(
    "fixed_tablet_types", [False, True], ids=["fixed_tablet_types=False", "fixed_tablet_types=True"]
)
class TestBootstrapTablets:
    def test_empty_system_tablets(self, fixed_tablet_types):
        assert generate_bootstrap(fixed_tablet_types, {}) == expected_bootstrap(fixed_tablet_types)

    def test_dbs_disabled(self, fixed_tablet_types):
        actual = generate_bootstrap(fixed_tablet_types, {"dbs_controller": {"enabled": False}})
        assert actual == expected_bootstrap(fixed_tablet_types)

    def test_dbs_enabled(self, fixed_tablet_types):
        expected = expected_bootstrap(fixed_tablet_types)
        expected.Tablet.add().CopyFrom(expected_tablet("DBS_CONTROLLER", 0x0100000000002004))
        actual = generate_bootstrap(fixed_tablet_types, {"dbs_controller": {"enabled": True}})
        assert actual == expected
