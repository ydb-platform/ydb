from types import SimpleNamespace as NS

import pytest

from ydb.apps.dstool.lib import common, grouptool
from ydb.apps.dstool.lib import dstool_cmd_vdisk_wipe as wipe
from ydb.apps.dstool.lib import dstool_cmd_cluster_workload_run as workload
from ydb.apps.dstool.lib import dstool_cmd_group_list as group_list


def test_static_group_listing():
    response = {'StorageGroups': [
        {'GroupId': '0', 'PoolName': 'static', 'GroupGeneration': '7',
         'ErasureSpecies': 'block-8-2', 'State': 'degraded:2', 'VDisks': [{}] * 12},
        {'GroupId': str(0x82000000)},
    ]}
    assert group_list.static_group_rows(response) == [{
        'GroupId': 0, 'PoolName': 'static', 'BoxId:PoolId': '-', 'Generation': 7,
        'ErasureSpecies': 'block-8-2', 'OperatingStatus': '-',
        'ViewerState': 'degraded:2', 'VDisks_TOTAL': 12,
    }]


@pytest.mark.parametrize("failed", range(4))
@pytest.mark.parametrize("erasure,size", [("block-4-2", 8), ("block-8-2", 12)])
def test_failure_model(failed, erasure, size):
    layout = {(0, domain, 0): domain < size - failed for domain in range(size)}
    assert grouptool.check_fail_model(layout, erasure) == (failed <= 2)
    # Multiple VDisks in the same failed domain consume only one domain failure.
    layout[0, size - 1, 1] = layout[0, size - 1, 0]
    assert grouptool.check_fail_model(layout, erasure) == (failed <= 2)


def test_unknown_erasure_is_refused():
    with pytest.raises(grouptool.UnsupportedErasureError, match="unsupported erasure"):
        grouptool.check_fail_model({(0, 0, 0): True}, "future-erasure")
    pool = NS(Geometry=NS(NumFailRealms=0, NumFailDomainsPerFailRealm=0, NumVDisksPerFailDomain=0), ErasureSpecies="future-erasure")
    with pytest.raises(grouptool.UnsupportedErasureError):
        grouptool.GroupMapper({}, pool)
    pool.ErasureSpecies = "block-8-2"
    mapper = grouptool.GroupMapper({}, pool)
    assert mapper.get_geometry() == (1, 12, 1)
    group = mapper.create_group()
    assert len(group) == 1 and len(group[0]) == 12
    assert all(len(domain) == 1 for domain in group[0])


def fixture(monkeypatch, failed, erasure="block-8-2"):
    group_id = 0x82000000
    slots = []
    for domain in range(12):
        ready = domain >= failed
        slots.append(NS(VSlotId=NS(NodeId=domain + 1, PDiskId=1, VSlotId=1000),
                        GroupId=group_id, GroupGeneration=1, FailRealmIdx=0, FailDomainIdx=domain, VDiskIdx=0,
                        Status="READY" if ready else "ERROR", Ready=ready, ReadOnly=False))
    group = NS(GroupId=group_id, GroupGeneration=1, BoxId=1, StoragePoolId=1,
               ErasureSpecies=erasure, VSlotId=[slot.VSlotId for slot in slots])
    pool = NS(BoxId=1, StoragePoolId=1, ErasureSpecies=erasure)
    config = NS(Group=[group], VSlot=slots, Node=[NS(NodeId=i, HostKey=NS(Fqdn="storage-%d" % i)) for i in range(1, 13)],
                PDisk=[NS(NodeId=i, PDiskId=1, ReadOnly=False) for i in range(1, 13)])
    monkeypatch.setattr(common, "fetch_base_config_and_storage_pools", lambda: {"BaseConfig": config, "StoragePools": [pool]})
    monkeypatch.setattr(common, "fetch_base_config", lambda: config)
    monkeypatch.setattr(common, "flush_cache", lambda: None)
    vdisks = {
        common.get_vslot_id(slot.VSlotId): {
            "VDiskId": {"GroupID": group_id, "GroupGeneration": 1, "Ring": 0, "Domain": slot.FailDomainIdx, "VDisk": 0},
            "Replicated": slot.Ready, "VDiskState": "OK" if slot.Ready else "PDiskError",
        } for slot in slots
    }
    monkeypatch.setattr(common, "fetch_json_info", lambda name: vdisks)

    def forbidden(*args, **kwargs):
        pytest.fail("No mutation may be dispatched by this test")
    monkeypatch.setattr(common, "invoke_bsc_request", forbidden)
    monkeypatch.setattr(wipe, "perform_request", forbidden)
    return group_id


@pytest.mark.parametrize("failed", range(4))
def test_wipe_dry_run_and_refusal(monkeypatch, capfd, failed):
    group_id = fixture(monkeypatch, failed)
    args = NS(vdisk_ids=["[%08x:1:0:11:0]" % group_id], force=False, run=failed >= 2, format="pretty")
    with pytest.raises(SystemExit) as exit_info:
        wipe.do(args)
    assert exit_info.value.code == 1
    output = capfd.readouterr()
    if failed >= 2:
        assert "failure model has failed" in output.out + output.err
    else:
        assert "not run by default" in output.out + output.err


def test_wipe_unknown_scheme_never_dispatches(monkeypatch):
    group_id = fixture(monkeypatch, 0, "future-erasure")
    args = NS(vdisk_ids=["[%08x:1:0:11:0]" % group_id], force=True, run=True, format="pretty")
    with pytest.raises(grouptool.UnsupportedErasureError):
        wipe.do(args)


@pytest.mark.parametrize("failed", range(4))
def test_workload_refuses_third_failure(monkeypatch, failed):
    fixture(monkeypatch, failed)

    class StopWorkload(BaseException):
        pass

    eligible = []

    def choose(actions):
        for name, (_, candidates) in actions:
            assert name == "wipe"
            eligible.extend(slot.FailDomainIdx for _, (_, slot) in candidates)
        raise StopWorkload()

    def sleep(_):
        raise StopWorkload()
    monkeypatch.setattr(workload.random, "choice", choose)
    monkeypatch.setattr(workload.time, "sleep", sleep)
    args = NS(enable_pdisk_encryption_keys_changes=False, disable_restarts=True, enable_kill_tablets=False,
              enable_kill_blob_depot=False, no_fail_model_check=False, disable_readonly=True, disable_evicts=True,
              disable_wipes=False, enable_restart_pdisks=False, enable_readonly_pdisks=False)
    with pytest.raises(StopWorkload):
        workload.do(args)
    assert (11 in eligible) == (failed < 2)
