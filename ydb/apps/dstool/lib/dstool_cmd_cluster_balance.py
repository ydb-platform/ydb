import ydb.apps.dstool.lib.common as common
import time
import sys
import random
from collections import defaultdict, Counter

description = 'Move vdisks out from overpopulated pdisks.'


def add_options(p):
    p.add_argument('--max-replicating-pdisks', type=int, help='Limit number of maximum replicating PDisks in the cluster')
    p.add_argument('--only-from-overpopulated-pdisks', action='store_true', help='Move vdisks out only from pdisks with over expected slot count')
    p.add_argument('--sort-by', choices=['slots', 'space_ratio'], default='slots', help='First to reassign disks with the most slots or with the highest space ratio')
    p.add_argument('--storage-pool', type=str, help='Storage pool to balance')
    p.add_argument('--max-donors-per-pdisk', type=int, default=0, help='Limit number of donors per pdisk')
    common.add_basic_format_options(p)


def build_pdisk_statistics(base_config, pdisk_map, vsolts):
    pdisks_statistics = {
        pdisk_id: {
            "PDiskId": pdisk_id,
            "AvailableSize": pdisk.PDiskMetrics.AvailableSize,
            "TotalSize": pdisk.PDiskMetrics.TotalSize,
            "CandidateVSlots": [],
            "DonorVSlots": [],
        }
        for pdisk_id, pdisk in pdisk_map.items()
        if pdisk.PDiskMetrics.TotalSize > 0  # pdisk works
    }
    for vslot in vsolts:
        pdisk_id = common.get_pdisk_id(vslot.VSlotId)
        pdisks_statistics[pdisk_id]["CandidateVSlots"].append(vslot)
    for vslot in base_config.VSlot:
        for donor in vslot.Donors:
            pdisk_id = common.get_pdisk_id(donor.VSlotId)
            pdisks_statistics[pdisk_id]["DonorVSlots"].append(donor)
    return pdisks_statistics


def do(args):
    while True:
        common.flush_cache()

        base_config = common.fetch_base_config()
        storage_pools = common.fetch_storage_pools()
        node_mon_map = common.fetch_node_mon_map({vslot.VSlotId.NodeId for vslot in base_config.VSlot})
        vslot_map = common.build_vslot_map(base_config)
        pdisk_map = common.build_pdisk_map(base_config)
        pdisk_usage = common.build_pdisk_usage_map(base_config, count_donors=False)
        pdisk_usage_w_donors = common.build_pdisk_usage_map(base_config, count_donors=True)

        storage_pool_names_map = common.build_storage_pool_names_map(storage_pools)
        group_id_to_storage_pool_name_map = {
            group_id: storage_pool_names_map[(group.BoxId, group.StoragePoolId)]
            for group_id, group in common.build_group_map(base_config).items()
            if (group.BoxId, group.StoragePoolId) != (0, 0)  # static group
        }

        vdisks_groups_count_map = defaultdict(int)
        for group in base_config.Group:
            num = sum(vslot.Status == 'READY' for vslot in common.vslots_of_group(group, vslot_map)) - len(group.VSlotId)
            vdisks_groups_count_map[num] += 1

        if any(k < -1 for k in vdisks_groups_count_map.keys()):
            common.print_if_not_quiet(args, 'There are groups with more than one non READY vslot, waiting...', sys.stdout)
            common.print_if_verbose(args, f'Number of non READY vdisks -> number of groups: {sorted(vdisks_groups_count_map.items())}', file=sys.stdout)
            time.sleep(15)
            continue

        if args.max_replicating_pdisks is not None:
            replicating_pdisks = set()
            for vslot in base_config.VSlot:
                if vslot.Status != 'READY' and vslot.Status != 'ERROR':
                    replicating_pdisks.add(common.get_pdisk_id(vslot.VSlotId))

            if len(replicating_pdisks) > args.max_replicating_pdisks:
                common.print_if_not_quiet(args, 'Waiting for %d pdisks to finish replication...' % (len(replicating_pdisks) - args.max_replicating_pdisks), sys.stdout)
                common.print_if_verbose(args, 'Replicating pdisks: ' + ', '.join('[%d:%d]' % x for x in sorted(replicating_pdisks)), file=sys.stdout)
                time.sleep(15)
                continue

        all_groups = common.select_groups(base_config)
        healthy_groups = common.filter_healthy_groups(all_groups, node_mon_map, base_config, vslot_map)
        unhealthy_groups = all_groups - healthy_groups
        if unhealthy_groups:
            common.print_if_verbose(args, 'Skipping vdisks from unhealthy groups: %s' % (unhealthy_groups), file=sys.stdout)

        healthy_vslots = [
            vslot
            for vslot in base_config.VSlot
            if vslot.GroupId in healthy_groups
        ]

        overpopulated_pdisks = set()
        for pdisk_id in pdisk_map.keys():
            if pdisk_map[pdisk_id].ExpectedSlotCount and pdisk_usage[pdisk_id] > pdisk_map[pdisk_id].ExpectedSlotCount:
                overpopulated_pdisks.add(pdisk_id)

        if not overpopulated_pdisks:
            common.print_if_not_quiet(args, 'No overpopulated pdisks found', sys.stdout)
            if args.only_from_overpopulated_pdisks:
                common.print_status(args, success=True, error_reason='')
                break

        healthy_vslots_from_overpopulated_pdisks = []
        for vslot in base_config.VSlot:
            pdisk_id = common.get_pdisk_id(vslot.VSlotId)
            if pdisk_id not in overpopulated_pdisks:
                continue
            if vslot.GroupId not in healthy_groups:
                continue

            healthy_vslots_from_overpopulated_pdisks.append(vslot)

        candidate_vslots = []
        if healthy_vslots_from_overpopulated_pdisks:
            common.print_if_not_quiet(args, f'Found {len(healthy_vslots_from_overpopulated_pdisks)} vdisks in healthy groups from overpopulated pdisks', sys.stdout)
            candidate_vslots = healthy_vslots_from_overpopulated_pdisks
        elif healthy_vslots and not args.only_from_overpopulated_pdisks:
            common.print_if_not_quiet(args, f'Found {len(healthy_vslots)} vdisks in healthy groups', sys.stdout)
            candidate_vslots = healthy_vslots

        if args.storage_pool is not None:
            existing_storage_pools = set(group_id_to_storage_pool_name_map.values())
            if args.storage_pool not in existing_storage_pools:
                print(f"Storage pool {args.storage_pool} not found in existing storage pools: {existing_storage_pools}")
                sys.exit(1)
            candidate_vslots = [vslot for vslot in candidate_vslots if group_id_to_storage_pool_name_map[vslot.GroupId] == args.storage_pool]
            common.print_if_not_quiet(args, f'Found {len(candidate_vslots)} vdisks in {args.storage_pool} sotrage pool', sys.stdout)

        if args.max_donors_per_pdisk > 0:
            donors_per_pdisk = common.build_donors_per_pdisk_map(base_config)
            candidate_vslots = [vslot for vslot in candidate_vslots if donors_per_pdisk[common.get_pdisk_id(vslot.VSlotId)] < args.max_donors_per_pdisk]
            common.print_if_not_quiet(args, f'Found {len(candidate_vslots)} vdisks with donors per pdisk < {args.max_donors_per_pdisk}', sys.stdout)

        if len(candidate_vslots) == 0:
            common.print_if_not_quiet(args, 'No vdisks suitable for relocation found, waiting..', sys.stdout)
            time.sleep(10)
            continue

        histo = Counter(pdisk_usage.values())
        common.print_if_verbose(args, 'Number of used slots -> number pdisks: ' + ' '.join('%d=>%d' % (k, histo[k]) for k in sorted(histo)), file=sys.stdout)

        def do_reassign(vslot, try_blocking):
            pdisk_id = common.get_pdisk_id(vslot.VSlotId)
            vslot_id = common.get_vslot_id(vslot.VSlotId)

            common.print_if_verbose(args, 'Checking to relocate vdisk from vslot %s on pdisk %s with slot usage %d' % (vslot_id, pdisk_id, pdisk_usage[pdisk_id]), file=sys.stdout)

            current_usage = pdisk_usage[pdisk_id]
            if not healthy_vslots_from_overpopulated_pdisks:
                for i in range(0, current_usage - 1):
                    if histo[i]:
                        break
                else:
                    return False

            def add_update_drive_status(request, pdisk, status):
                cmd = request.Command.add().UpdateDriveStatus
                cmd.HostKey.NodeId = pdisk.NodeId
                cmd.PDiskId = pdisk.PDiskId
                cmd.Status = status

            def add_reassign_cmd(request, vslot):
                cmd = request.Command.add().ReassignGroupDisk
                cmd.GroupId = vslot.GroupId
                cmd.GroupGeneration = vslot.GroupGeneration
                cmd.FailRealmIdx = vslot.FailRealmIdx
                cmd.FailDomainIdx = vslot.FailDomainIdx
                cmd.VDiskIdx = vslot.VDiskIdx

            request = common.kikimr_bsconfig.TConfigRequest(Rollback=True)
            index = len(request.Command)
            add_reassign_cmd(request, vslot)
            response = common.invoke_bsc_request(request)
            if len(response.Status) != 1 or not response.Status[0].Success:
                return False
            item = response.Status[index].ReassignedItem[0]
            pdisk_from = item.From.NodeId, item.From.PDiskId
            pdisk_to = item.To.NodeId, item.To.PDiskId
            if pdisk_usage[pdisk_to] + 1 > pdisk_usage[pdisk_from] - 1:
                if pdisk_usage_w_donors[pdisk_to] + 1 > pdisk_map[pdisk_to].ExpectedSlotCount:
                    common.print_if_not_quiet(
                        args,
                        'NOTICE: Attempted to reassign vdisk from pdisk [%d:%d] to pdisk [%d:%d] with slot usage %d and slot limit %d on latter',
                        *pdisk_from, *pdisk_to, pdisk_usage_w_donors[pdisk_to], pdisk_map[pdisk_to].ExpectedSlotCount)
                    return False

                if not try_blocking:
                    return False
                request = common.kikimr_bsconfig.TConfigRequest(Rollback=True)
                inactive = []
                for pdisk in base_config.PDisk:
                    check_pdisk_id = common.get_pdisk_id(pdisk)
                    disk_is_better = pdisk_usage_w_donors[check_pdisk_id] + 1 <= pdisk_map[check_pdisk_id].ExpectedSlotCount
                    if disk_is_better:
                        if not healthy_vslots_from_overpopulated_pdisks and pdisk_usage[check_pdisk_id] + 1 > pdisk_usage[pdisk_id] - 1:
                            disk_is_better = False
                        if healthy_vslots_from_overpopulated_pdisks:
                            disk_is_better = False

                    if not disk_is_better:
                        add_update_drive_status(request, pdisk, common.kikimr_bsconfig.EDriveStatus.INACTIVE)
                        inactive.append(pdisk)
                index = len(request.Command)
                add_reassign_cmd(request, vslot)
                for pdisk in inactive:
                    add_update_drive_status(request, pdisk, pdisk.DriveStatus)
                response = common.invoke_bsc_request(request)
                if len(response.Status) != 1 or not response.Status[index].Success:
                    return False

            request.Rollback = args.dry_run
            response = common.invoke_bsc_request(request)

            if response.Status[index].Success:
                from_pdisk_id = common.get_pdisk_id(response.Status[index].ReassignedItem[0].From)
                to_pdisk_id = common.get_pdisk_id(response.Status[index].ReassignedItem[0].To)
                common.print_if_not_quiet(
                    args,
                    'Relocated vdisk from pdisk [%d:%d] to pdisk [%d:%d] with slot usages (%d -> %d)' % (*from_pdisk_id, *to_pdisk_id, pdisk_usage[from_pdisk_id], pdisk_usage[to_pdisk_id]),
                    file=sys.stdout)

            if not common.is_successful_bsc_response(response):
                common.print_request_result(args, request, response)
                sys.exit(1)

            return True
        # end of do_reassign()

        vslots_ordered_groups_to_reassign = None
        if args.sort_by == 'slots':
            vslots_by_pdisk_slot_usage = defaultdict(list)
            for vslot in candidate_vslots:
                pdisk_id = common.get_pdisk_id(vslot.VSlotId)
                pdisk_slot_usage = pdisk_usage[pdisk_id]
                vslots_by_pdisk_slot_usage[pdisk_slot_usage].append(vslot)
            vslots_ordered_groups_to_reassign = [vslots for _, vslots in sorted(vslots_by_pdisk_slot_usage.items(), reverse=True)]
        elif args.sort_by == 'space_ratio':
            pdisks = {
                pdisk_id: {
                    "FreeSpaceRatio": float(pdisk.PDiskMetrics.AvailableSize) / float(pdisk.PDiskMetrics.TotalSize),
                    "CandidateVSlots": [],
                }
                for pdisk_id, pdisk in pdisk_map.items()
                if pdisk.PDiskMetrics.TotalSize > 0  # pdisk works
            }
            for vslot in candidate_vslots:
                pdisk_id = common.get_pdisk_id(vslot.VSlotId)
                pdisks[pdisk_id]["CandidateVSlots"].append(vslot)
            vslots_ordered_groups_to_reassign = [info["CandidateVSlots"] for _, info in sorted(list(pdisks.items()), key=lambda x: x[1]["FreeSpaceRatio"])]

        for vslots in vslots_ordered_groups_to_reassign:
            random.shuffle(vslots)
            for vslot in vslots:
                if do_reassign(vslot, False):
                    break
            else:
                for vslot in vslots:
                    if do_reassign(vslot, True):
                        break
                else:
                    continue
            break
        else:
            common.print_status(args, success=True, error_reason='')
            break
