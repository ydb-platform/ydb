import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.grouptool as grouptool
import sys

description = 'Check groups'


def add_options(p):
    common.add_group_ids_option(p, required=True)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--failure-model', action='store_true', help='Check failure model of groups')
    common.add_basic_format_options(p)


def check_failure_model(args):
    base_config_and_storage_pools = common.fetch_base_config_and_storage_pools()
    base_config = base_config_and_storage_pools['BaseConfig']
    storage_pools = base_config_and_storage_pools['StoragePools']

    pdisks_map = common.build_pdisk_map(base_config)
    storage_pool_groups_map = common.build_storage_pool_groups_map(base_config, args.group_ids)

    node_to_fdom = {
        node.NodeId: common.Location.from_location(node.Location, node.NodeId)
        for node in base_config.Node
    }

    node_to_dc_rack = {
        node.NodeId: node.Location.DataCenter + '::' + node.Location.Rack
        for node in base_config.Node
    }

    vslot_to_coordinates = {
        (id_.NodeId, id_.PDiskId, id_.VSlotId): (vslot.FailRealmIdx, vslot.FailDomainIdx, vslot.VDiskIdx)
        for vslot in base_config.VSlot
        for id_ in [vslot.VSlotId]
    }

    boxes = {}
    for box_id in set(sp.BoxId for sp in storage_pools):
        boxes[box_id] = [
            node_to_fdom[pdisk.NodeId]._replace(node=pdisk.NodeId, disk=pdisk.PDiskId)
            for pdisk in base_config.PDisk
            if pdisk.BoxId == box_id
        ]

    def geom_key(g):
        return g.RealmLevelBegin, g.RealmLevelEnd, g.DomainLevelBegin, g.DomainLevelEnd

    box_geoms = {}
    for box_id, geom in set((sp.BoxId, geom_key(sp.Geometry)) for sp in storage_pools):
        box_geoms[box_id, geom] = grouptool.decompose_location_map_by_levels(geom, boxes[box_id])

    success = True
    error_reason = ''
    for sp in storage_pools:
        sp_name = sp.Name
        location_id_map = box_geoms[sp.BoxId, geom_key(sp.Geometry)]

        for group in storage_pool_groups_map[sp.BoxId, sp.StoragePoolId]:
            location_map = {}
            vslot_map = {}
            for id_ in group.VSlotId:
                c = vslot_to_coordinates[id_.NodeId, id_.PDiskId, id_.VSlotId]
                location_map[c] = location_id_map[node_to_fdom[id_.NodeId]._replace(node=id_.NodeId, disk=id_.PDiskId)]
                vslot_map[c] = id_.NodeId, id_.PDiskId, id_.VSlotId

            # the following conditions must be met for the disks of every group:
            # 1. RealmPrefix is the same for all of the disks in the group
            # 2. All disks in the same realm must have the same RealmInfix; RealmInfix must differ for different realms
            # 3. DomainPrefix must be the same for all of the disks in same realm (but may or may not differ for different realms)
            # 4. DomainInfix must be different for every fail domain of the realm

            e = grouptool.check_group(location_map.items())
            if e:
                error_reason += 'Group %d from %s is ill-formed: %s\n' % (group.GroupId, sp_name, e)
                success = False
                prev = None
                row = []
                for c, (node_id, _, _) in sorted(vslot_map.items()):
                    if prev is not None and c[0] != prev[0]:
                        error_reason += ' '.join(row)
                        error_reason += '\n'
                        row = []
                    row.append(node_to_dc_rack[node_id])
                    prev = c
                if row:
                    error_reason += ' '.join(row)
                    error_reason += '\n'

            for id_ in group.VSlotId:
                pdisk = pdisks_map[id_.NodeId, id_.PDiskId]
                if not common.pdisk_matches_storage_pool(pdisk, sp):
                    error_reason += 'Group %d from %s contains incorrect PDisk\n' % (group.GroupId, sp_name)
                    success = False

    if not success:
        return False, error_reason
    return True, ''


def do(args):
    error_reason = ''
    success = True
    if args.failure_model:
        success, error_reason = check_failure_model(args)

    common.print_status(args, success, error_reason)
    if not success:
        sys.exit(1)
