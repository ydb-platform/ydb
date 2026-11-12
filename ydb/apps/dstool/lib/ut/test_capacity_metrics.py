from types import SimpleNamespace

import pytest

from ydb.apps.dstool.lib import common, table
from ydb.apps.dstool.lib import dstool_cmd_group_list as group_list
from ydb.apps.dstool.lib import dstool_cmd_pool_list as pool_list
from ydb.apps.dstool.lib import dstool_cmd_vdisk_list as vdisk_list


@pytest.mark.parametrize(
    'command, use_grpc',
    [(vdisk_list, False), (group_list, False), (pool_list, False), (group_list, True)],
    ids=['vdisk', 'group', 'pool', 'group-grpc'])
@pytest.mark.parametrize('group_size_in_units', [0, 1, 3, 20])
@pytest.mark.parametrize('expected_slot_size', [0, 101])
@pytest.mark.parametrize('slot_size_in_units', [0, 2, 5])
@pytest.mark.parametrize('enforced_slot_size', [0, 96])
@pytest.mark.parametrize('user_chunk_pool_size', [None, 0, 200])
def test_rounded_slot_size_in_list(command, use_grpc, group_size_in_units, expected_slot_size,
                                   slot_size_in_units, enforced_slot_size, user_chunk_pool_size, monkeypatch):
    base_config = common.kikimr_bsconfig.TBaseConfig()
    base_config.Node.add(NodeId=1).HostKey.Fqdn = 'node-1'
    pdisk = base_config.PDisk.add(NodeId=1, PDiskId=1, ExpectedSlotSize=expected_slot_size)
    pdisk.PDiskMetrics.EnforcedDynamicSlotSize = enforced_slot_size
    pdisk.PDiskMetrics.TotalSize = 1800
    if user_chunk_pool_size is not None:
        pdisk.PDiskMetrics.UserChunkPoolSize = user_chunk_pool_size
    pdisk.PDiskMetrics.ExpectedSlotCount = 10
    pdisk.PDiskMetrics.SlotSizeInUnits = slot_size_in_units
    group = base_config.Group.add(GroupId=0x80000001, GroupSizeInUnits=group_size_in_units,
                                  BoxId=1, StoragePoolId=1)
    vslot = base_config.VSlot.add(GroupId=group.GroupId, Status='READY')
    vslot.VSlotId.NodeId = 1
    vslot.VSlotId.PDiskId = 1
    vslot.VSlotId.VSlotId = 1
    group.VSlotId.add().CopyFrom(vslot.VSlotId)
    units = max(1, group_size_in_units)
    if expected_slot_size:
        rounded_quota = (enforced_slot_size or expected_slot_size) * units
        if user_chunk_pool_size is not None:
            rounded_quota = min(rounded_quota, user_chunk_pool_size)
    else:
        slot_units = max(1, slot_size_in_units)
        rounded_quota = enforced_slot_size * ((units + slot_units - 1) // slot_units)
    vslot.VDiskMetrics.AllocatedSize = 24
    vslot.VDiskMetrics.AvailableSize = max(0, rounded_quota - 24)
    storage_pool = common.kikimr_bsconfig.TDefineStoragePool(BoxId=1, StoragePoolId=1, Name='pool')
    data = {'BaseConfig': base_config, 'StoragePools': [storage_pool]}
    monkeypatch.setattr(common, 'fetch_base_config_and_storage_pools', lambda **kwargs: data)
    monkeypatch.setattr(common, 'has_explicit_grpc_endpoints', lambda: use_grpc)
    monkeypatch.setattr(common, 'fetch_storage_state', lambda **kwargs: group_list._convert_legacy_storage_state(data))
    rows = []
    monkeypatch.setattr(table.TableOutput, 'dump', lambda self, result, args: rows.extend(result))
    command.do(SimpleNamespace(show_pdisk_status=False, show_vdisk_usage=True, show_vdisk_status=False,
                               show_group_status=False, show_vdisk_estimated_usage=True, all_columns=False,
                               virtual_groups_only=False))

    assert len(rows) == 1
    row = rows[0]
    assert row['SlotSize' if command is vdisk_list else 'Limit'] == rounded_quota
    assert row['UsedSize'] == 24
    assert row['AvailableSize'] == max(0, rounded_quota - 24)
    if command is pool_list:
        assert row['EstimatedUsage'] == (pytest.approx(24 / rounded_quota) if rounded_quota else 0.0)
    else:
        assert row['VDiskRawUsage'] == (pytest.approx(24 / rounded_quota) if rounded_quota else None)
