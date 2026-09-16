from types import SimpleNamespace

import pytest

from ydb.apps.dstool.lib import dstool_cmd_cluster_balance as balance


@pytest.fixture
def strategy():
    args = SimpleNamespace(
        group_ids=None, storage_pool=None, only_from_overpopulated_pdisks=False,
        max_donors_per_pdisk=0, max_replicating_pdisks=None,
        prefer_less_occupied_rack=False, with_attention_to_replication=False,
        dry_run=False, quiet=True,
    )
    info = balance.ClusterInfo()
    info.base_config = balance.common.kikimr_bsconfig.TBaseConfig()
    for node in range(1, 4):
        info.base_config.PDisk.add(NodeId=node, PDiskId=1, DriveStatus=balance.common.kikimr_bs3.EDriveStatus.ACTIVE)
        vslot = info.base_config.VSlot.add(GroupId=node, Status='READY')
        vslot.VSlotId.NodeId = node
        vslot.VSlotId.PDiskId = 1
        vslot.VSlotId.VSlotId = 1
    info.pdisk_map = balance.common.build_pdisk_map(info.base_config)
    info.pdisk_usage = {(1, 1): 6, (2, 1): 5, (3, 1): 0}
    info.pdisk_usage_w_donors = dict(info.pdisk_usage)
    info.expected_slot_count_map = {(1, 1): 10, (2, 1): 10, (3, 1): 0}
    info.pdisk_slot_size_in_units_map = {}
    info.group_map = {}
    info.group_id_to_storage_pool_name_map = {}
    info.vdisks_groups_count_map = {}
    groups = balance.GroupsInfo()
    groups.healthy_groups = {1, 2, 3}
    result = balance.VSlotBalancingStrategy(args, info, groups)
    result.calculate_extra_info()
    return result


def response_to(node, index=0):
    response = balance.common.kikimr_bsconfig.TConfigResponse(Success=True)
    for _ in range(index + 1):
        status = response.Status.add(Success=True)
    item = status.ReassignedItem.add()
    item.From.NodeId, item.From.PDiskId = 1, 1
    item.To.NodeId, item.To.PDiskId = node, 1
    return response


@pytest.fixture
def inferred_settings_strategy(strategy, monkeypatch):
    base_config = balance.common.kikimr_bsconfig.TBaseConfig()
    base_config.PDisk.add(NodeId=1, PDiskId=1, ExpectedSlotCount=8)
    base_config.PDisk.add(NodeId=3, PDiskId=1, ExpectedSlotCount=4)
    for i, group_size_in_units in enumerate((8, 1)):
        group = base_config.Group.add(GroupId=0x80000001 + i, GroupSizeInUnits=group_size_in_units)
        vslot = base_config.VSlot.add(GroupId=group.GroupId, Status='READY', Ready=True)
        vslot.VSlotId.NodeId = 1
        vslot.VSlotId.PDiskId = 1
        vslot.VSlotId.VSlotId = i + 1
        group.VSlotId.add().CopyFrom(vslot.VSlotId)
    monkeypatch.setattr(balance.common, 'fetch_base_config', lambda: base_config)
    monkeypatch.setattr(balance.common, 'fetch_storage_pools', lambda: [])
    return strategy


@pytest.mark.parametrize('expected_slot_count, slot_count, overpopulated', [(8, 16, False), (16, 8, True)])
def test_source_metrics_slot_count_precedence(inferred_settings_strategy, expected_slot_count, slot_count, overpopulated):
    strategy = inferred_settings_strategy
    source = balance.common.fetch_base_config().PDisk[0]
    source.ExpectedSlotCount = expected_slot_count
    source.PDiskMetrics.SlotCount = slot_count
    strategy.cluster_info = balance.ClusterInfo.collect_cluster_info()
    strategy.calculate_extra_info()
    assert strategy.cluster_info.pdisk_usage[1, 1] == 9
    assert strategy.cluster_info.expected_slot_count_map[1, 1] == slot_count
    assert ((1, 1) in strategy.overpopulated_pdisks) == overpopulated


def test_destination_metrics_slot_count_precedence(inferred_settings_strategy, monkeypatch):
    strategy = inferred_settings_strategy
    destination = balance.common.fetch_base_config().PDisk[1]
    destination.PDiskMetrics.SlotCount = 10
    strategy.cluster_info = balance.ClusterInfo.collect_cluster_info()
    strategy.calculate_extra_info()
    assert strategy.cluster_info.expected_slot_count_map[3, 1] == 10
    vslot = strategy.cluster_info.base_config.VSlot[0]
    assert strategy.cluster_info.get_vslot_weight_on_pdisk(vslot.GroupId, (3, 1)) == 8
    requests = []

    def invoke(request):
        requests.append(request.Rollback)
        return response_to(3)

    monkeypatch.setattr(balance.common, 'invoke_bsc_request', invoke)
    assert strategy.reassign_vslot(vslot, False)
    assert requests == [True, False]


@pytest.mark.parametrize('expected_slot_count, donors, accepted', [(0, 0, True), (1, 0, True), (1, 1, False)])
@pytest.mark.parametrize('overpopulated', [False, True])
def test_destination_capacity(strategy, monkeypatch, expected_slot_count, donors, accepted, overpopulated):
    info = strategy.cluster_info
    # No metrics or ExpectedSlotCount means the inferred expected_slot_count is zero.
    destination = info.pdisk_map[3, 1]
    if expected_slot_count:
        destination.ExpectedSlotCount = expected_slot_count
    info.expected_slot_count_map[3, 1] = balance.common.get_pdisk_inferred_settings(destination)[0]
    info.pdisk_usage_w_donors[3, 1] = donors
    if overpopulated:
        info.expected_slot_count_map[1, 1] = 5
    strategy.calculate_extra_info()
    requests = []

    def invoke(request):
        requests.append(request.Rollback)
        return response_to(3)

    monkeypatch.setattr(balance.common, 'invoke_bsc_request', invoke)
    assert strategy.reassign_vslot(info.base_config.VSlot[0], False) == accepted
    assert requests == ([True, False] if accepted else [True])


@pytest.mark.parametrize('strategy_type', [balance.VSlotBalancingStrategy, balance.SpaceRatioBalancingStrategy])
@pytest.mark.parametrize('only_overpopulated', [False, True])
def test_unmovable_overpopulated_sources(strategy, monkeypatch, strategy_type, only_overpopulated):
    info = strategy.cluster_info
    info.expected_slot_count_map[1, 1] = 5
    # Give the regular candidate a size mismatch to check that source priority wins.
    info.pdisk_slot_size_in_units_map[2, 1] = 2
    for pdisk in info.base_config.PDisk:
        pdisk.PDiskMetrics.TotalSize = 100
        pdisk.PDiskMetrics.AvailableSize = 50
    strategy.args.only_from_overpopulated_pdisks = only_overpopulated
    strategy = strategy_type(strategy.args, info, strategy.groups_info)
    attempts = []

    def reassign(vslot, blocking):
        attempts.append((vslot.GroupId, blocking))
        return vslot.GroupId == 2

    monkeypatch.setattr(strategy, 'reassign_vslot', reassign)
    result = balance.balance_iteration(strategy.args, strategy, 1)
    assert attempts[:2] == [(1, False), (1, True)]
    if only_overpopulated:
        assert result is True
        assert len(attempts) == 2
    else:
        assert result is None
        assert attempts[2:] == [(2, False)]


@pytest.mark.parametrize('only_overpopulated', [False, True])
def test_cluster_with_unknown_capacity(strategy, monkeypatch, only_overpopulated):
    info = strategy.cluster_info
    info.expected_slot_count_map = {
        pdisk_id: balance.common.get_pdisk_inferred_settings(pdisk)[0]
        for pdisk_id, pdisk in info.pdisk_map.items()
    }
    assert set(info.expected_slot_count_map.values()) == {0}
    strategy.args.only_from_overpopulated_pdisks = only_overpopulated
    requests = []

    def invoke(request):
        requests.append(request.Rollback)
        return response_to(3)

    monkeypatch.setattr(balance.common, 'invoke_bsc_request', invoke)
    result = balance.balance_iteration(strategy.args, strategy, 1)
    assert not strategy.overpopulated_pdisks
    if only_overpopulated:
        assert result is True
        assert requests == []
    else:
        assert result is None
        assert requests == [True, False]


@pytest.mark.parametrize('unknown_capacity', [False, True])
@pytest.mark.parametrize('dry_run', [False, True])
@pytest.mark.parametrize('failed_command_index', [None, 0, 1, 2, 3, 4])
def test_blocking_reassignment(strategy, monkeypatch, dry_run, failed_command_index, unknown_capacity):
    strategy.args.dry_run = dry_run
    info = strategy.cluster_info
    if unknown_capacity:
        info.expected_slot_count_map = {
            pdisk_id: balance.common.get_pdisk_inferred_settings(pdisk)[0]
            for pdisk_id, pdisk in info.pdisk_map.items()
        }
    else:
        info.expected_slot_count_map[3, 1] = 10
    strategy.calculate_extra_info()
    requests = []

    def invoke(request):
        copy = balance.common.kikimr_bsconfig.TConfigRequest()
        copy.CopyFrom(request)
        requests.append(copy)
        if len(requests) == 1:
            return response_to(2)  # Does not improve balance; force blocking.
        commands = request.Command
        assert len(commands) == 5
        for i, node in enumerate((1, 2)):
            inactive = commands[i].UpdateDriveStatus
            restored = commands[3 + i].UpdateDriveStatus
            assert (inactive.HostKey.NodeId, inactive.PDiskId) == (node, 1)
            assert inactive.Status == balance.common.kikimr_bs3.EDriveStatus.INACTIVE
            assert (restored.HostKey.NodeId, restored.PDiskId) == (node, 1)
            assert restored.Status == balance.common.kikimr_bs3.EDriveStatus.ACTIVE
        assert commands[2].ReassignGroupDisk.GroupId == 1
        response = response_to(3, index=2)
        response.Status.add(Success=True)
        response.Status.add(Success=True)
        if failed_command_index is not None:
            # BSC stops processing commands at the first failure.
            del response.Status[failed_command_index + 1:]
            response.Status[failed_command_index].Success = False
            response.Status[failed_command_index].ClearField('ReassignedItem')
            response.Success = False
            response.ErrorDescription = 'Simulated command failure'
        return response

    monkeypatch.setattr(balance.common, 'invoke_bsc_request', invoke)
    assert strategy.reassign_vslot(strategy.cluster_info.base_config.VSlot[0], True) == (failed_command_index is None)
    assert [r.Rollback for r in requests] == ([True, True, dry_run] if failed_command_index is None else [True, True])
    if failed_command_index is None:
        assert requests[1].Command == requests[2].Command
