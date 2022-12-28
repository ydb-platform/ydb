import ydb.apps.dstool.lib.common as common
from ydb.apps.dstool.lib.common import kikimr_bsconfig
from operator import itemgetter, attrgetter
from collections import OrderedDict
import ydb.core.protos.blobstorage_pb2 as kikimr_bs


class IdBase(object):
    def __init__(self):
        super().__init__()

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._data() == other._data()

    def __hash__(self):
        return hash(self._data())


class NodeId(IdBase):
    def __init__(self, value):
        super().__init__()
        assert isinstance(value, int)
        self.value = value

    def _data(self):
        return self.value

    def __str__(self):
        return '%d' % self.value

    def __repr__(self):
        return str(self)


class PDiskId(IdBase):
    def __init__(self, node_id, value):
        super().__init__()
        assert isinstance(node_id, NodeId)
        assert isinstance(value, int)
        self.node_id, self.value = node_id, value

    def _data(self):
        return self.node_id, self.value

    def __str__(self):
        return '%d:%d' % (self.node_id.value, self.value)

    def __repr__(self):
        return str(self)


class VSlotId(IdBase):
    def __init__(self, pdisk_id, value):
        super().__init__()
        assert isinstance(pdisk_id, PDiskId)
        assert isinstance(value, int)
        self.pdisk_id, self.value = pdisk_id, value

    @staticmethod
    def from_proto(vslot_id):
        return VSlotId(PDiskId(NodeId(vslot_id.NodeId), vslot_id.PDiskId), vslot_id.VSlotId)

    def _data(self):
        return self.pdisk_id, self.value

    def __str__(self):
        return '%d:%d:%d' % (self.pdisk_id.node_id.value, self.pdisk_id.value, self.value)

    def __repr__(self):
        return str(self)


class GroupId(IdBase):
    def __init__(self, group_id):
        super().__init__()
        assert isinstance(group_id, int)
        self.value = group_id

    def _data(self):
        return self.value

    def __str__(self):
        return '%d' % self.value

    def __repr__(self):
        return str(self)


class StoragePoolId(IdBase):
    def __init__(self, box_id, storage_pool_id):
        super().__init__()
        assert isinstance(box_id, int)
        assert isinstance(storage_pool_id, int)
        self.box_id, self.storage_pool_id = box_id, storage_pool_id

    def _data(self):
        return self.box_id, self.storage_pool_id

    def __str__(self):
        return '%d.%d' % (self.box_id, self.storage_pool_id)

    def __repr__(self):
        return str(self)


class EntityBase(object):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return '{%s}' % ' '.join(
            '%s# %s' % (k, v)
            for k, v in self._params().items()
            if not isinstance(v, str) or v != ''
        )

    def __repr__(self):
        return str(self)


class Node(EntityBase):
    def __init__(self, base, node_mon_endpoint=None):
        super().__init__()
        self.base = base
        self.node_mon_endpoint = node_mon_endpoint
        self.pdisks_of_node = set()  # a set of references to PDisk structure

    def _params(self):
        return OrderedDict(
            NodeId=NodeId(self.base.NodeId),
            Mon=self.node_mon_endpoint or '',
        )


class PDisk(EntityBase):
    def __init__(self, base, node):
        super().__init__()
        self.base, self.node = base, node
        self.vslots_of_pdisk = set()
        node.pdisks_of_node.add(self)
        self.mon_state = None

    def _params(self):
        return OrderedDict(
            PDiskId=PDiskId(NodeId(self.base.NodeId), self.base.PDiskId),
            Path=self.base.Path,
            MonState=self.mon_state or '',
        )


class VSlot(EntityBase):
    def __init__(self, base, pdisk):
        super().__init__()
        self.base, self.pdisk = base, pdisk
        self.group = None  # a group this VSlot belongs to, if not donor; otherwise None
        self.donors = set()
        self.acceptor = None  # None for an active disk and VSlot reference for donor one
        pdisk.vslots_of_pdisk.add(self)
        self.status = kikimr_bs.EVDiskStatus.Value(self.base.Status) if self.base.Status else None
        self.mon_fetched = False
        self.mon_state = None
        self.mon_replicated = None

    def _params(self):
        return OrderedDict(
            VSlotId=VSlotId.from_proto(self.base.VSlotId),
            VDiskId='[%08x:%d:%d:%d:%d]' % (self.base.GroupId, self.base.GroupGeneration, self.base.FailRealmIdx, self.base.FailDomainIdx, self.base.VDiskIdx),
            Kind=self.base.VDiskKind,
            Status=self.base.Status,
            DonorCount='itself' if self.acceptor else str(len(self.donors)),
        )


class Group(EntityBase):
    def __init__(self, base, layout):
        super().__init__()
        self.base = base
        self.storage_pool = None
        self.vslots_of_group = [
            layout.vslots[vslot_id]
            for vslot_id in map(VSlotId.from_proto, self.base.VSlotId)
        ]
        for vslot in self.vslots_of_group:
            vslot.group = self

    def _params(self):
        return OrderedDict(
            GroupId='%d:%d' % (self.base.GroupId, self.base.GroupGeneration),
            Erasure=self.base.ErasureSpecies,
            VSlots='[%s]' % ' '.join(str(VSlotId.from_proto(id_)) for id_ in self.base.VSlotId),
            StoragePoolId=StoragePoolId(self.base.BoxId, self.base.StoragePoolId),
            Name=self.storage_pool.base.Name if self.storage_pool else 'static',
        )


class StoragePool(EntityBase):
    def __init__(self, base):
        super().__init__()
        self.base = base
        self.groups = set()

    def _params(self):
        return OrderedDict(
            StoragePoolId=StoragePoolId(self.base.BoxId, self.base.StoragePoolId),
            Name=self.base.Name,
        )


class BlobStorageLayout(object):
    def __init__(self):
        # fetch essential storage configuration from blb storage controller
        base_config, storage_pools = itemgetter('BaseConfig', 'StoragePools')(common.fetch_base_config_and_storage_pools())

        # build a map of nodes
        self.nodes = {
            NodeId(node.NodeId): Node(node)
            for node in base_config.Node
        }

        # then go the pdisks
        self.pdisks = {
            pdisk_id: PDisk(pdisk, self.nodes[pdisk_id.node_id])
            for pdisk in base_config.PDisk
            for pdisk_id in [PDiskId(NodeId(pdisk.NodeId), pdisk.PDiskId)]
        }

        # then the vslots
        self.vslots = {
            vslot_id: VSlot(vslot, self.pdisks[vslot_id.pdisk_id])
            for vslot in base_config.VSlot
            for vslot_id in [VSlotId.from_proto(vslot.VSlotId)]
        }

        # fix the donors
        donors = {}
        for vslot in self.vslots.values():
            for donor in vslot.base.Donors:
                id_ = VSlotId.from_proto(donor.VSlotId)
                vdisk_id = donor.VDiskId
                donor_vslot = VSlot(
                    kikimr_bsconfig.TBaseConfig.TVSlot(
                        VSlotId=donor.VSlotId,
                        GroupId=vdisk_id.GroupID,
                        GroupGeneration=vdisk_id.GroupGeneration,
                        FailRealmIdx=vdisk_id.Ring,
                        FailDomainIdx=vdisk_id.Domain,
                        VDiskIdx=vdisk_id.VDisk,
                        VDiskMetrics=donor.VDiskMetrics,
                    ),
                    self.pdisks[id_.pdisk_id]
                )
                donor_vslot.acceptor = vslot
                vslot.donors.add(donor_vslot)
                donors[id_] = donor_vslot

        # add donors to self.vslots
        self.vslots.update(donors)

        # the groups
        self.groups = {
            GroupId(group.GroupId): Group(group, self)
            for group in base_config.Group
        }

        # aaand the storage pools
        self.storage_pools = {
            StoragePoolId(sp.BoxId, sp.StoragePoolId): StoragePool(sp)
            for sp in storage_pools
        }

        # fix group mapping
        for group in self.groups.values():
            sp_id = StoragePoolId(group.base.BoxId, group.base.StoragePoolId)
            if sp_id != StoragePoolId(0, 0):
                sp = self.storage_pools[sp_id]
                group.storage_pool = sp
                sp.groups.add(group)

    def fetch_node_mon_endpoints(self, nodes=None):
        """ Fetch monitoring endpoints for specific nodes (if set) or for all of them. """
        if isinstance(nodes, str) and nodes == 'storage':
            nodes = {pdisk_id.node_id for pdisk_id in self.pdisks}

        def get_nodes():
            for node_id in nodes if nodes is not None else self.nodes:
                assert isinstance(node_id, NodeId)
                if self.nodes[node_id].node_mon_endpoint is None:
                    yield node_id.value

        for node_id, sysinfo in common.fetch_json_info('sysinfo', get_nodes()).items():
            node_id = NodeId(node_id)
            node = self.nodes[node_id]
            for ep in sysinfo.get('Endpoints', []):
                if ep['Name'] == 'http-mon':
                    node.node_mon_endpoint = sysinfo['Host'] + ep['Address']

    def fetch_pdisk_mon_state(self, nodes=None):
        possible_nodes = {pdisk_id.node_id for pdisk_id, pdisk in self.pdisks.items() if pdisk.mon_state is None}
        nodes = nodes & possible_nodes if nodes is not None else possible_nodes
        for (node_id, pdisk_id), pdiskinfo in common.fetch_json_info('pdiskinfo', (x.value for x in nodes), enums=0).items():
            pdisk_id = PDiskId(NodeId(node_id), pdisk_id)
            self.pdisks[pdisk_id].mon_state = pdiskinfo.get('State')

    def fetch_vdisk_mon_state(self, nodes=None):
        possible_nodes = {vslot_id.pdisk_id.node_id for vslot_id, vslot in self.vslots.items() if not vslot.mon_fetched}
        nodes = nodes & possible_nodes if nodes is not None else possible_nodes
        json_get = itemgetter('GroupID', 'Ring', 'Domain', 'VDisk')
        proto_get = attrgetter('GroupId', 'FailRealmIdx', 'FailDomainIdx', 'VDiskIdx')
        for (node_id, pdisk_id, vslot_id), vdiskinfo in common.fetch_json_info('vdiskinfo', (x.value for x in nodes), enums=0).items():
            vslot = self.vslots.get(VSlotId(PDiskId(NodeId(node_id), pdisk_id), vslot_id))
            if vslot and json_get(vdiskinfo['VDiskId']) == proto_get(vslot.base):
                vslot.mon_fetched = True
                vslot.mon_state = vdiskinfo.get('VDiskState')
                vslot.mon_replicated = vdiskinfo.get('Replicated')
