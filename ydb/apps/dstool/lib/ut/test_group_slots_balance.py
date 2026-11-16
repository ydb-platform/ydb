import argparse

import pytest

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
