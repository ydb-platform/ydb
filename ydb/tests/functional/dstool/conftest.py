import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig

# XXX: setting of pytest_plugins should work if specified directly in test modules
# but somehow it does not
#
# for ydb_{cluster, database, ...} fixture family
pytest_plugins = 'ydb.tests.library.fixtures'


class BaseConfigBuilder:
    """Helper to build mock TBaseConfig protobuf messages for unit testing."""

    def __init__(self):
        self._base_config = kikimr_bsconfig.TBaseConfig()
        self._storage_pools = []

    def add_node(self, node_id=1, fqdn='localhost', ic_port=19001):
        node = self._base_config.Node.add()
        node.NodeId = node_id
        node.HostKey.Fqdn = fqdn
        node.HostKey.IcPort = ic_port
        return self

    def add_pdisk(self, node_id=1, pdisk_id=1, expected_slot_count=0, slot_size_in_units=0, enforced_dynamic_slot_size=0):
        pdisk = self._base_config.PDisk.add()
        pdisk.NodeId = node_id
        pdisk.PDiskId = pdisk_id
        pdisk.NumStaticSlots = 0
        pdisk.PDiskConfig.ExpectedSlotCount = expected_slot_count
        pdisk.PDiskConfig.SlotSizeInUnits = slot_size_in_units
        pdisk.PDiskMetrics.SlotCount = expected_slot_count
        pdisk.PDiskMetrics.SlotSizeInUnits = slot_size_in_units
        pdisk.PDiskMetrics.EnforcedDynamicSlotSize = enforced_dynamic_slot_size
        return self

    def add_vslot(self, node_id, pdisk_id, vslot_id, group_id,
                  group_generation=0, fail_realm_idx=0, fail_domain_idx=0, vdisk_idx=0,
                  status='READY', allocated_size=0, available_size=0):
        vslot = self._base_config.VSlot.add()
        vslot.VSlotId.NodeId = node_id
        vslot.VSlotId.PDiskId = pdisk_id
        vslot.VSlotId.VSlotId = vslot_id
        vslot.GroupId = group_id
        vslot.GroupGeneration = group_generation
        vslot.FailRealmIdx = fail_realm_idx
        vslot.FailDomainIdx = fail_domain_idx
        vslot.VDiskIdx = vdisk_idx
        vslot.Status = status
        vslot.VDiskMetrics.AllocatedSize = allocated_size
        vslot.VDiskMetrics.AvailableSize = available_size
        return self

    def add_group(self, group_id, erasure_species='none',
                  group_size_in_units=0, vslot_ids=None,
                  box_id=1, storage_pool_id=1):
        group = self._base_config.Group.add()
        group.GroupId = group_id
        group.GroupGeneration = 1
        group.ErasureSpecies = erasure_species
        group.GroupSizeInUnits = group_size_in_units
        group.BoxId = box_id
        group.StoragePoolId = storage_pool_id
        for node_id, pdisk_id, vs_id in (vslot_ids or []):
            slot_ref = group.VSlotId.add()
            slot_ref.NodeId = node_id
            slot_ref.PDiskId = pdisk_id
            slot_ref.VSlotId = vs_id
        return self

    def add_storage_pool(self, box_id=1, storage_pool_id=1, name='pool-1',
                         erasure_species='none', kind='hdd',
                         default_group_size_in_units=0):
        sp = kikimr_bsconfig.TDefineStoragePool()
        sp.BoxId = box_id
        sp.StoragePoolId = storage_pool_id
        sp.Name = name
        sp.ErasureSpecies = erasure_species
        sp.Kind = kind
        sp.DefaultGroupSizeInUnits = default_group_size_in_units
        self._storage_pools.append(sp)
        return self

    def update_pdisk(self, node_id, pdisk_id, slot_size_in_units=None, enforced_dynamic_slot_size=None):
        for pdisk in self._base_config.PDisk:
            if pdisk.NodeId == node_id and pdisk.PDiskId == pdisk_id:
                if slot_size_in_units is not None:
                    pdisk.PDiskConfig.SlotSizeInUnits = slot_size_in_units
                    pdisk.PDiskMetrics.SlotSizeInUnits = slot_size_in_units
                if enforced_dynamic_slot_size is not None:
                    pdisk.PDiskMetrics.EnforcedDynamicSlotSize = enforced_dynamic_slot_size
                break
        return self

    def update_vslot(self, node_id, pdisk_id, vslot_id, available_size=None, allocated_size=None):
        for vslot in self._base_config.VSlot:
            if (vslot.VSlotId.NodeId == node_id and vslot.VSlotId.PDiskId == pdisk_id and vslot.VSlotId.VSlotId == vslot_id):
                if available_size is not None:
                    vslot.VDiskMetrics.AvailableSize = available_size
                if allocated_size is not None:
                    vslot.VDiskMetrics.AllocatedSize = allocated_size
                break
        return self

    def update_group(self, group_id, group_size_in_units=None):
        for group in self._base_config.Group:
            if group.GroupId == group_id:
                if group_size_in_units is not None:
                    group.GroupSizeInUnits = group_size_in_units
                break
        return self

    def build(self):
        return dict(BaseConfig=self._base_config, StoragePools=self._storage_pools)
