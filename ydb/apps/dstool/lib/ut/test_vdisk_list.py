import ydb.apps.dstool.lib.common as common
from ydb.apps.dstool.lib import dstool_cmd_vdisk_list as vdisk_list

GROUP_ID = 0x82000299


def make_base_config():
    config = common.kikimr_bsconfig.TBaseConfig()
    for node_id in (1, 2):
        node = config.Node.add(NodeId=node_id)
        node.HostKey.Fqdn = 'node%d' % node_id
    for node_id in (1, 2):
        pdisk = config.PDisk.add(
            NodeId=node_id, PDiskId=1, BoxId=1,
            DriveStatus=common.kikimr_bs3.EDriveStatus.ACTIVE,
            DecommitStatus=common.kikimr_bs3.EDecommitStatus.NONE,
            Path='/dev/disk/by-id/test')
        pdisk.PDiskMetrics.ExpectedSlotCount = 1
        pdisk.PDiskMetrics.SlotSizeInUnits = 1
        pdisk.PDiskMetrics.EnforcedDynamicSlotSize = 1000
    group = config.Group.add(
        GroupId=GROUP_ID, GroupGeneration=3, ErasureSpecies='none',
        BoxId=1, StoragePoolId=1, GroupSizeInUnits=1)
    group.VSlotId.add(NodeId=1, PDiskId=1, VSlotId=1)
    vslot = config.VSlot.add(
        GroupId=GROUP_ID, GroupGeneration=3, Status='READY', VDiskKind='Default',
        FailRealmIdx=0, FailDomainIdx=0, VDiskIdx=0)
    vslot.VSlotId.NodeId = 1
    vslot.VSlotId.PDiskId = 1
    vslot.VSlotId.VSlotId = 1
    vslot.VDiskMetrics.AllocatedSize = 10
    vslot.VDiskMetrics.AvailableSize = 20
    donor = vslot.Donors.add()
    donor.VDiskId.GroupID = GROUP_ID
    donor.VDiskId.GroupGeneration = 2
    donor.VDiskId.Ring = 0
    donor.VDiskId.Domain = 0
    donor.VDiskId.VDisk = 0
    donor.VSlotId.NodeId = 2
    donor.VSlotId.PDiskId = 1
    donor.VSlotId.VSlotId = 7
    donor.VDiskMetrics.AllocatedSize = 100
    donor.VDiskMetrics.AvailableSize = 200
    return config


def make_storage_pools():
    storage_pool = common.kikimr_bsconfig.TDefineStoragePool(
        BoxId=1, StoragePoolId=1, Name='TestPool')
    return [storage_pool]


def test_vdisk_list_without_donors():
    rows = vdisk_list.build_rows(make_base_config(), make_storage_pools(), show_donors=False)
    assert len(rows) == 1
    row = rows[0]
    assert row['IsDonor'] is False
    assert row['VDiskId'] == '[%08x:3:0:0:0]' % GROUP_ID
    assert row['NodeId'] == 1
    assert row['FQDN'] == 'node1'
    assert row['VSlotStatus'] == 'READY'
    assert row['UsedSize'] == 10
    assert row['PoolName'] == 'TestPool'


def test_vdisk_list_with_donors():
    rows = vdisk_list.build_rows(make_base_config(), make_storage_pools(), show_donors=True)
    assert len(rows) == 2
    main_row, donor_row = rows
    assert main_row['IsDonor'] is False
    assert donor_row['IsDonor'] is True
    assert donor_row['VDiskId'] == '[%08x:2:0:0:0]' % GROUP_ID
    assert donor_row['GroupGeneration'] == 2
    assert donor_row['NodeId'] == 2
    assert donor_row['FQDN'] == 'node2'
    assert donor_row['VSlotId'] == 7
    assert donor_row['VSlotStatus'] is None
    assert donor_row['UsedSize'] == 100
    assert donor_row['AvailableSize'] == 200
    assert donor_row['PDiskDecommitStatus'] == 'NONE'


def test_build_donors_per_pdisk_map():
    donors = common.build_donors_per_pdisk_map(make_base_config())
    assert donors == {(2, 1): 1}
