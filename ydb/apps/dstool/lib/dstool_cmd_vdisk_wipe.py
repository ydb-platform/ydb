import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.grouptool as grouptool
import sys

description = 'Wipe vdisks'


def add_options(p):
    common.add_vdisk_ids_option(p, required=True)
    p.add_argument('--force', action='store_true', help='Force execution in spite of group safety check results')
    common.add_ignore_degraded_group_check_option(p)
    common.add_ignore_disintegrated_group_check_option(p)
    common.add_ignore_failure_model_group_check_option(p)
    p.add_argument('--run', action='store_true', help='Run command (by default the command is not run)')
    common.add_basic_format_options(p)


def create_request(args, vslot):
    return common.create_wipe_request(args=args, vslot=vslot)


def perform_request(request):
    return common.invoke_wipe_request(request)


def is_successful_response(response):
    return common.is_successful_bsc_response(response)


def do(args):
    base_config_and_storage_pools = common.fetch_base_config_and_storage_pools()
    base_config = base_config_and_storage_pools['BaseConfig']

    node_fqdn_map = common.build_node_fqdn_map(base_config)
    storage_pools = base_config_and_storage_pools['StoragePools']
    storage_pools_map = common.build_storage_pools_map(storage_pools)

    vslot_status = {
        common.get_vslot_id(vslot.VSlotId): vslot.Status == 'READY'
        for vslot in base_config.VSlot
    }
    for vslot in base_config.VSlot:
        if not vslot.Status:
            vslot_id = common.get_vslot_id(vslot.VSlotId)
            node_fqdn = node_fqdn_map[vslot.VSlotId.NodeId]
            try:
                json_data = common.fetch('viewer/json/vdiskinfo', dict(node_id=0, enums=1), node_fqdn)
                vdisks = [
                    vslot_data
                    for vslot_data in json_data.get('VDiskStateInfo', [])
                    if tuple(map(vslot_data.__getitem__, ('NodeId', 'PDiskId', 'VDiskSlotId'))) == vslot_id
                ]
                if not vdisks:
                    raise Exception('No matching VDisks')
                elif len(vdisks) > 1:
                    raise Exception('Too many matching VDisks')
                vdisk = vdisks[0]
                vslot_status[vslot_id] = vdisk['VDiskState'] == 'OK' and vdisk['Replicated'] is True
            except Exception as e:
                common.print_if_not_quiet(args, 'Failed to query VDisk status for group %d host %s: %s' % (vslot.GroupId, node_fqdn, e), file=sys.stderr)

    vslot_coord = {
        common.get_vslot_id(vslot.VSlotId): (vslot.FailRealmIdx, vslot.FailDomainIdx, vslot.VDiskIdx)
        for vslot in base_config.VSlot
    }

    def get_vslots():
        vslot_map = {}
        for v in base_config.VSlot:
            vslot_map['[%08x:_:%d:%d:%d]' % (v.GroupId, v.FailRealmIdx, v.FailDomainIdx, v.VDiskIdx)] = v
            vslot_map['[%08x:%d:%d:%d:%d]' % (v.GroupId, v.GroupGeneration, v.FailRealmIdx, v.FailDomainIdx, v.VDiskIdx)] = v

        def find_vslot(vdisk_id):
            res = vslot_map.get(vdisk_id)
            if res is None:
                common.print_status(args, success=False, error_reason="Couldn't find VDisk with id %s" % vdisk_id)
                sys.exit(1)
            return res
        return (find_vslot(vdisk_id) for vdisk_id in args.vdisk_ids)

    wipe_set = {
        (vslot.GroupId,) + v: vslot
        for vslot in get_vslots()
        for v in [common.get_vslot_id(vslot.VSlotId)]
    }

    if not wipe_set:
        common.print_status(args, success=False, error_reason="Couldn't get any VDisks")
        sys.exit(1)

    def do_failure_model_group_checks():
        success = True
        for group in base_config.Group:
            group_vslots = set(map(common.get_vslot_id, group.VSlotId))
            wiped = group_vslots & set(v[1:] for v in wipe_set)
            if not wiped:
                continue  # ignore groups we are not going to wipe
            sp = storage_pools_map[group.BoxId, group.StoragePoolId]
            current = {
                vslot_coord[v]: vslot_status[v]
                for v in group_vslots
            }
            new = {
                vslot_coord[v]: vslot_status[v] and v not in wiped
                for v in group_vslots
            }
            current_status = grouptool.check_fail_model(current, sp.ErasureSpecies)
            new_status = grouptool.check_fail_model(new, sp.ErasureSpecies)
            if current_status and not new_status:
                success = False
                common.print_if_verbose(args, 'Group %d will lose its data during the operation' % group.GroupId, file=sys.stderr)
            elif not new_status:
                success = False
                common.print_if_verbose(args, 'Group %d will remain degraded after the operation' % group.GroupId, file=sys.stderr)
        return success

    if not do_failure_model_group_checks() and not args.force:
        common.print_status(args, success=False, error_reason='Check of groups failure model has failed')
        sys.exit(1)

    for item in sorted(wipe_set):
        common.print_if_verbose(args, 'About to wipe disk with GroupId# %d NodeId# %d PDiskId# %d VSlotId# %d' % item, file=sys.stderr)

    if args.run:
        success = True
        error_reason = ''
        for item in sorted(wipe_set):
            vslot = wipe_set[item]

            request = create_request(args, vslot)
            response = perform_request(request)
            if not is_successful_response(response):
                success = False
                error_reason += 'Request has failed: \n{0}\n{1}\n'.format(request, response)

        common.print_status(args, success, error_reason)
        if not success:
            sys.exit(1)
    else:
        common.print_result(args.format, 'error', 'For safety reasons the command is not run by default, use --run to run the command', file=sys.stderr)
        sys.exit(1)
