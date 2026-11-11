import argparse
import json
from pathlib import Path

import pytest
import yatest.common

from ydb.apps.dstool.lib import dstool_cmd_cluster_balance as balance


@pytest.fixture
def cluster(monkeypatch):
    config = balance.common.kikimr_bsconfig.TBaseConfig()
    for node in range(1, 6):
        config.PDisk.add(NodeId=node, PDiskId=1, BoxId=1, Type=1, DriveStatus=1, ExpectedSlotCount=10)
    for index in range(3):
        group = config.Group.add(GroupId=0x80000001 + index, GroupGeneration=1, BoxId=1, StoragePoolId=1)
        for node in range(1, 4):
            slot = config.VSlot.add(
                GroupId=group.GroupId, GroupGeneration=1, FailDomainIdx=node - 1, Status='READY', Ready=True
            )
            slot.VSlotId.NodeId = node
            slot.VSlotId.PDiskId = 1
            slot.VSlotId.VSlotId = index + 1
            slot.VDiskMetrics.Replicated = True
            slot.VDiskMetrics.State = balance.common.whiteboard_disk_states.EVDiskState.OK
            group.VSlotId.add().CopyFrom(slot.VSlotId)
    pool = balance.common.kikimr_bsconfig.TDefineStoragePool(BoxId=1, StoragePoolId=1, Name='pool')
    monkeypatch.setattr(balance.common, 'fetch_base_config', lambda: config)
    monkeypatch.setattr(balance.common, 'fetch_storage_pools', lambda: [pool])
    monkeypatch.setattr(balance.common, 'flush_cache', lambda: None)
    monkeypatch.setattr(balance.random, 'shuffle', lambda items: None)
    return config


def options(*extra):
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--quiet', action='store_true')
    balance.add_options(parser)
    return parser.parse_args(['--sort-by=group_slots', '--storage-pool=pool', '--max-iterations=1', *extra])


def make_strategy(args):
    info = balance.ClusterInfo.collect_cluster_info()
    groups = balance.GroupsInfo.collect_groups_info(info)
    args.group_ids = set(info.group_map)
    strategy = balance.GroupVSlotsBalancingStrategy(args, info, groups)
    assert not strategy.calculate_extra_info()
    return strategy


@pytest.mark.parametrize('mode', ['slots', 'space_ratio', 'group_slots'])
def test_cli_accepts_balancing_modes(mode):
    assert options('--sort-by=' + mode).sort_by == mode


@pytest.mark.parametrize('excluded', ['full', 'inactive', 'same_group', 'same_node'])
def test_group_slots_chooses_next_eligible_target(cluster, monkeypatch, excluded):
    strategy = make_strategy(options())
    target = (4, 1)
    if excluded == 'full':
        strategy.cluster_info.pdisk_usage_w_donors[target] = 10
    elif excluded == 'inactive':
        strategy.cluster_info.pdisk_map[target].DriveStatus = balance.common.kikimr_bs3.EDriveStatus.INACTIVE
    elif excluded == 'same_group':
        strategy.group_pdisks[0x80000001].add(target)
    else:
        strategy.group_nodes[0x80000001].add(4)
    requests = []

    def invoke(request):
        requests.append(request)
        return balance.common.kikimr_bsconfig.TConfigResponse(Success=True)

    monkeypatch.setattr(balance.common, 'invoke_bsc_request', invoke)
    assert strategy.reassign_vslot(cluster.VSlot[0], False)
    assert len(requests) == 1
    assert requests[0].Command[0].ReassignGroupDisk.TargetPDiskId.NodeId == 5


def test_group_slots_no_request_when_all_targets_are_full(cluster, monkeypatch):
    for pdisk in cluster.PDisk[3:]:
        pdisk.NumStaticSlots = 10
    strategy = make_strategy(options())

    def unexpected_request(request):
        pytest.fail('A full destination must be rejected before contacting BSC')

    monkeypatch.setattr(balance.common, 'invoke_bsc_request', unexpected_request)
    assert not strategy.reassign_vslot(cluster.VSlot[0], False)


@pytest.mark.parametrize('verbose', [False, True])
def test_group_slots_rejection_output(cluster, monkeypatch, capfd, verbose):
    strategy = make_strategy(options(*(['--verbose'] if verbose else [])))
    capfd.readouterr()
    response = balance.common.kikimr_bsconfig.TConfigResponse(Success=False, ErrorDescription='placement rejected')
    monkeypatch.setattr(balance.common, 'invoke_bsc_request', lambda request: response)
    assert not strategy.reassign_vslot(cluster.VSlot[0], False)
    captured = capfd.readouterr()
    assert ('placement rejected' in captured.err) == verbose
    assert 'error' not in captured.out


@pytest.mark.parametrize('dry_run', [False, True])
def test_group_slots_request_flags(cluster, dry_run):
    args = options(*(['--dry-run'] if dry_run else []))
    args.prefer_less_occupied_rack = True
    args.with_attention_to_replication = True
    strategy = make_strategy(args)
    request = strategy._make_reassign_request(cluster.VSlot[1], (5, 1))
    cmd = request.Command[0].ReassignGroupDisk
    assert request.Rollback == dry_run
    assert (cmd.GroupId, cmd.GroupGeneration, cmd.FailDomainIdx) == (0x80000001, 1, 1)
    assert cmd.PreferLessOccupiedRack
    assert cmd.WithAttentionToReplication


@pytest.mark.parametrize('scenario', ['move', 'dry_run', 'full', 'rejected', 'retry', 'unhealthy', 'balanced'])
def test_group_slots_canonical(cluster, monkeypatch, capfd, scenario):
    # Node 4 is full; a valid destination must be selected beyond it.
    cluster.PDisk[3].NumStaticSlots = 10
    if scenario == 'full':
        cluster.PDisk[4].NumStaticSlots = 10
    if scenario == 'unhealthy':
        cluster.VSlot[0].Ready = False
    if scenario == 'balanced':
        for group_index, slot_index, node in [(0, 2, 4), (1, 0, 5), (2, 1, 4)]:
            group = cluster.Group[group_index]
            slot = cluster.VSlot[group_index * 3 + slot_index]
            slot.VSlotId.NodeId = node
            group.VSlotId[slot_index].CopyFrom(slot.VSlotId)
        cluster.PDisk[3].NumStaticSlots = 0
    args = options(*(['--dry-run'] if scenario == 'dry_run' else []))
    requests = []

    def invoke(request):
        requests.append(str(request))
        success = scenario != 'rejected' and not (scenario == 'retry' and len(requests) == 1)
        response = balance.common.kikimr_bsconfig.TConfigResponse(Success=success)
        if not success:
            response.ErrorDescription = 'placement rejected'
        return response

    monkeypatch.setattr(balance.common, 'invoke_bsc_request', invoke)
    balance.do(args)
    captured = capfd.readouterr()
    if scenario in ('full', 'balanced'):
        assert not requests
    elif scenario == 'retry':
        assert len(requests) == 2
    elif scenario != 'rejected':
        assert len(requests) == 1
    assert captured.err == ('success\n' if scenario in ('full', 'rejected', 'balanced') else '')
    path = Path(yatest.common.output_path('balance.json'))
    path.write_text(json.dumps(dict(stdout=captured.out, stderr=captured.err, requests=requests), indent=2) + '\n')
    return yatest.common.canonical_file(str(path), local=True, universal_lines=True)
