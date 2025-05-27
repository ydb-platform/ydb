import ydb.apps.dstool.lib.common as common
import time
import sys
import random
from collections import defaultdict, Counter

description = 'Move vdisks out from overpopulated pdisks.'


class Constants:
    WAITING_TIME = 15
    TIME_BERWEEN_REASSIGNINGS = 0

    @classmethod
    def apply_args(cls, args):
        if args.waiting_time:
            cls.WAITING_TIME = args.waiting_time
        if args.time_between_reassignings:
            cls.TIME_BERWEEN_REASSIGNINGS = args.time_between_reassignings


def add_options(p):
    p.add_argument('--max-replicating-pdisks', type=int, help='Limit number of maximum replicating PDisks in the cluster')
    p.add_argument('--only-from-overpopulated-pdisks', action='store_true', help='Move vdisks out only from pdisks with over expected slot count')
    p.add_argument(
        '--sort-by',
        choices=['slots', 'space_ratio'],
        default='slots',
        help='First to reassign disks with the most slots or with the highest space ratio or with the most vslots in the group'
    )
    g = p.add_mutually_exclusive_group()
    g.add_argument('--storage-pool', type=str, help='Storage pool to balance')
    common.add_group_ids_option(g)
    p.add_argument('--max-donors-per-pdisk', type=int, default=0, help='Limit number of donors per pdisk')
    p.add_argument('--allow-same-node', action='store_true', help='Allow to relocate vdisks from one group to the same node')
    p.add_argument('--waiting-time', type=int, default=Constants.WAITING_TIME, help='Time to wait when there are no vdisks to reassign')
    p.add_argument('--time-between-reassignings', type=int, default=Constants.TIME_BERWEEN_REASSIGNINGS, help='Time to wait between reassignings')
    common.add_basic_format_options(p)


class ClusterInfo:
    def __init__(self):
        self.base_config = None
        self.storage_pools = None
        self.node_mon_map = None
        self.vslot_map = None
        self.pdisk_map = None
        self.pdisk_usage = None
        self.storage_pool_names_map = None
        self.group_id_to_storage_pool_name_map = None
        self.vdisks_groups_count_map = None
        self.replicating_pdisks = None
        self.pdisk_usage_w_donors = None

    @staticmethod
    def collect_cluster_info(count_replicating_pdisks=False):
        info = ClusterInfo()
        info.base_config = common.fetch_base_config()
        info.storage_pools = common.fetch_storage_pools()
        info.node_mon_map = common.fetch_node_mon_map({vslot.VSlotId.NodeId for vslot in info.base_config.VSlot})
        info.vslot_map = common.build_vslot_map(info.base_config)
        info.pdisk_map = common.build_pdisk_map(info.base_config)
        info.pdisk_usage = common.build_pdisk_usage_map(info.base_config, count_donors=False)
        info.pdisk_usage_w_donors = common.build_pdisk_usage_map(info.base_config, count_donors=True)

        info.storage_pool_names_map = common.build_storage_pool_names_map(info.storage_pools)
        info.group_id_to_storage_pool_name_map = {
            group_id: info.storage_pool_names_map[(group.BoxId, group.StoragePoolId)]
            for group_id, group in common.build_group_map(info.base_config).items()
            if (group.BoxId, group.StoragePoolId) != (0, 0)  # static group
        }

        info.vdisks_groups_count_map = defaultdict(int)
        for group in info.base_config.Group:
            num = sum(vslot.Status == 'READY' for vslot in common.vslots_of_group(group, info.vslot_map)) - len(group.VSlotId)
            info.vdisks_groups_count_map[num] += 1
        return info

    def list_replicating_pdisks(self):
        if self.replicating_pdisks is not None:
            return self.replicating_pdisks

        self.replicating_pdisks = set()
        for vslot in self.base_config.VSlot:
            if vslot.Status != 'READY' and vslot.Status != 'ERROR':
                self.replicating_pdisks.add(common.get_pdisk_id(vslot.VSlotId))
        return self.replicating_pdisks


class GroupsInfo:
    def __init__(self):
        self.all_groups = None
        self.healthy_groups = None
        self.unhealthy_groups = None

    @staticmethod
    def collect_groups_info(cluster_info):
        groups_info = GroupsInfo()
        groups_info.all_groups = common.select_groups(cluster_info.base_config)
        groups_info.healthy_groups = common.filter_healthy_groups(groups_info.all_groups, cluster_info.node_mon_map, cluster_info.base_config, cluster_info.vslot_map)
        groups_info.unhealthy_groups = groups_info.all_groups - groups_info.healthy_groups
        groups_info.healthy_vslots = [
            vslot
            for vslot in cluster_info.base_config.VSlot
            if vslot.GroupId in groups_info.healthy_groups
        ]
        return groups_info

    def filter_healthy_vslots(self, vslots):
        return [vslot for vslot in vslots if vslot.GroupId in self.healthy_groups]


def list_overpopulated_pdisks(cluster_info):
    overpopulated_pdisks = set()
    for pdisk_id in cluster_info.pdisk_map.keys():
        expected_slot_count = cluster_info.pdisk_map[pdisk_id].ExpectedSlotCount
        pdisk_usage = cluster_info.pdisk_usage[pdisk_id]
        if expected_slot_count and pdisk_usage > expected_slot_count:
            overpopulated_pdisks.add(pdisk_id)
    return overpopulated_pdisks


def filter_vslots_by_group_ids(vslots, group_ids):
    if group_ids is None:
        return vslots
    return [vslot for vslot in vslots if vslot.GroupId in group_ids]


def filter_vslots_by_pdisks(vslots, pdisks):
    if pdisks is None:
        return vslots
    return [vslot for vslot in vslots if common.get_pdisk_id(vslot.VSlotId) in pdisks]


def filter_vslots_by_donors_per_pdisk(vslots, cluster_info, max_donors_per_pdisk):
    if max_donors_per_pdisk == 0:
        return vslots
    donors_per_pdisk = common.build_donors_per_pdisk_map(cluster_info.base_config)
    return [vslot for vslot in vslots if donors_per_pdisk[common.get_pdisk_id(vslot.VSlotId)] < max_donors_per_pdisk]


class IBalancingStrategy:
    def __init__(self, args, cluster_info, groups_info):
        self.args = args
        self.cluster_info = cluster_info
        self.groups_info = groups_info

    def verify_cluster_state(self):
        return False

    def check_waiting_conditions(self):
        return False

    def check_success_conditions(self):
        return False

    def calculate_extra_info(self):
        return False

    def list_candidate_vslots(self):
        return self.groups_info.healthy_vslots, False

    def filter_must_first_vslots(self, candidate_vslots):
        return candidate_vslots

    def order_candidate_vslots(self, candidate_vslots):
        return [candidate_vslots]

    def reassign_vslot(self, vslot, try_blocking):
        return False


class BalancingStrategy(IBalancingStrategy):
    def __init__(self, args, cluster_info, groups_info):
        super().__init__(args, cluster_info, groups_info)
        self.histo = None
        self.healthy_vslots_from_overpopulated_pdisks = None

    def verify_cluster_state(self):
        existing_storage_pools = set(self.cluster_info.group_id_to_storage_pool_name_map.values())
        if self.args.storage_pool is not None and self.args.storage_pool not in existing_storage_pools:
            print(f"Storage pool {self.args.storage_pool} not found in existing storage pools: {list(sorted(existing_storage_pools))}")
            return True
        return False

    def check_waiting_conditions(self):
        if any(k < -1 for k in self.cluster_info.vdisks_groups_count_map.keys()):
            common.print_if_not_quiet(self.args, 'There are groups with more than one non READY vslot, waiting...', sys.stdout)
            groups_count_str = ', '.join(f'{k}: {v}' for k, v in sorted(self.cluster_info.vdisks_groups_count_map.items()))
            common.print_if_verbose(self.args, f'Number of non READY vdisks -> number of groups: {groups_count_str}', file=sys.stdout)
            return True

        if self.args.max_replicating_pdisks is not None:
            replicating_pdisks = self.cluster_info.list_replicating_pdisks()
            if len(replicating_pdisks) > self.args.max_replicating_pdisks:
                over_replication_count = len(replicating_pdisks) - self.args.max_replicating_pdisks
                common.print_if_not_quiet(self.args, f'Waiting for {over_replication_count} pdisks to finish replication...', sys.stdout)
                replicating_pdisks_str = ', '.join(f'[{x[0]}:{x[1]}]' for x in sorted(replicating_pdisks))
                common.print_if_verbose(self.args, f'Replicating pdisks: {replicating_pdisks_str}', file=sys.stdout)
                return True

    def calculate_extra_info(self):
        self.histo = Counter(self.cluster_info.pdisk_usage.values())
        candidate_vslots = self.cluster_info.base_config.VSlot
        candidate_vslots = filter_vslots_by_group_ids(candidate_vslots, self.args.group_ids)
        candidate_vslots = self.filter_must_first_vslots(candidate_vslots)
        self.healthy_vslots_from_overpopulated_pdisks = candidate_vslots
        common.print_if_verbose(self.args, 'Number of used slots -> number pdisks: ' + ' '.join('%d=>%d' % (k, self.histo[k]) for k in sorted(self.histo)), file=sys.stdout)
        return False

    def filter_must_first_vslots(self, candidate_vslots):
        if self.args.only_from_overpopulated_pdisks:
            overpopulated_pdisks = list_overpopulated_pdisks(self.cluster_info)
            return filter_vslots_by_pdisks(candidate_vslots, overpopulated_pdisks)
        return candidate_vslots

    def list_candidate_vslots(self):
        candidate_vslots = self.cluster_info.base_config.VSlot
        candidate_vslots = filter_vslots_by_group_ids(candidate_vslots, self.args.group_ids)
        candidate_vslots = self.filter_must_first_vslots(candidate_vslots)
        if not candidate_vslots:
            return [], True
        candidate_vslots = self.groups_info.filter_healthy_vslots(candidate_vslots)
        candidate_vslots = filter_vslots_by_donors_per_pdisk(candidate_vslots, self.cluster_info, self.args.max_donors_per_pdisk)
        return candidate_vslots, False

    def order_candidate_vslots(self, candidate_vslots):
        return []

    def _add_update_drive_status(self, request, pdisk, status):
        cmd = request.Command.add().UpdateDriveStatus
        cmd.HostKey.NodeId = pdisk.NodeId
        cmd.PDiskId = pdisk.PDiskId
        cmd.Status = status

    def _add_reassign_cmd(self, request, vslot):
        cmd = request.Command.add().ReassignGroupDisk
        cmd.GroupId = vslot.GroupId
        cmd.GroupGeneration = vslot.GroupGeneration
        cmd.FailRealmIdx = vslot.FailRealmIdx
        cmd.FailDomainIdx = vslot.FailDomainIdx
        cmd.VDiskIdx = vslot.VDiskIdx

    def reassign_vslot(self, vslot, try_blocking):
        pdisk_id = common.get_pdisk_id(vslot.VSlotId)
        vslot_id = common.get_vslot_id(vslot.VSlotId)

        pdisk_usage = self.cluster_info.pdisk_usage
        pdisk_usage_w_donors = self.cluster_info.pdisk_usage_w_donors
        pdisk_map = self.cluster_info.pdisk_map
        histo = self.histo

        common.print_if_verbose(self.args, 'Checking to relocate vdisk from vslot %s on pdisk %s with slot usage %d' % (vslot_id, pdisk_id, pdisk_usage[pdisk_id]), file=sys.stdout)

        current_usage = pdisk_usage[pdisk_id]
        if not self.args.only_from_overpopulated_pdisks:
            for i in range(0, current_usage - 1):
                if histo[i]:
                    break
            else:
                return False

        request = common.kikimr_bsconfig.TConfigRequest(Rollback=True)
        index = len(request.Command)
        self._add_reassign_cmd(request, vslot)
        response = common.invoke_bsc_request(request)
        if len(response.Status) != 1 or not response.Status[0].Success:
            return False
        item = response.Status[index].ReassignedItem[0]
        pdisk_from = item.From.NodeId, item.From.PDiskId
        pdisk_to = item.To.NodeId, item.To.PDiskId
        if pdisk_usage[pdisk_to] + 1 > pdisk_usage[pdisk_from] - 1:
            if pdisk_usage_w_donors[pdisk_to] + 1 > pdisk_map[pdisk_to].ExpectedSlotCount:
                common.print_if_not_quiet(
                    self.args,
                    'NOTICE: Attempted to reassign vdisk from pdisk [%d:%d] to pdisk [%d:%d] with slot usage %d and slot limit %d on latter',
                    *pdisk_from, *pdisk_to, pdisk_usage_w_donors[pdisk_to], pdisk_map[pdisk_to].ExpectedSlotCount)
                return False

            if not try_blocking:
                return False
            request = common.kikimr_bsconfig.TConfigRequest(Rollback=True)
            inactive = []
            for pdisk in self.cluster_info.base_config.PDisk:
                check_pdisk_id = common.get_pdisk_id(pdisk)
                disk_is_better = pdisk_usage_w_donors[check_pdisk_id] + 1 <= pdisk_map[check_pdisk_id].ExpectedSlotCount
                if disk_is_better:
                    if not self.healthy_vslots_from_overpopulated_pdisks and pdisk_usage[check_pdisk_id] + 1 > pdisk_usage[pdisk_id] - 1:
                        disk_is_better = False
                    if self.healthy_vslots_from_overpopulated_pdisks:
                        disk_is_better = False

                if not disk_is_better:
                    self._add_update_drive_status(request, pdisk, common.kikimr_bsconfig.EDriveStatus.INACTIVE)
                    inactive.append(pdisk)
            index = len(request.Command)
            self._add_reassign_cmd(request, vslot)
            for pdisk in inactive:
                self._add_update_drive_status(request, pdisk, pdisk.DriveStatus)
            response = common.invoke_bsc_request(request)
            if len(response.Status) != 1 or not response.Status[index].Success:
                return False

        request.Rollback = self.args.dry_run
        response = common.invoke_bsc_request(request)

        if response.Status[index].Success:
            from_pdisk_id = common.get_pdisk_id(response.Status[index].ReassignedItem[0].From)
            to_pdisk_id = common.get_pdisk_id(response.Status[index].ReassignedItem[0].To)
            common.print_if_not_quiet(
                self.args,
                'Relocated vdisk from pdisk [%d:%d] to pdisk [%d:%d] with slot usages (%d -> %d)' % (*from_pdisk_id, *to_pdisk_id, pdisk_usage[from_pdisk_id], pdisk_usage[to_pdisk_id]),
                file=sys.stdout)

        if not common.is_successful_bsc_response(response):
            common.print_request_result(self.args, request, response)
            sys.exit(1)

        return True


def order_vslots_by_pdisk_usage(vslots, pdisk_usage):
    vslots_by_pdisk_slot_usage = defaultdict(list)
    for vslot in vslots:
        pdisk_id = common.get_pdisk_id(vslot.VSlotId)
        pdisk_slot_usage = pdisk_usage[pdisk_id]
        vslots_by_pdisk_slot_usage[pdisk_slot_usage].append(vslot)
    return [vslots for _, vslots in sorted(vslots_by_pdisk_slot_usage.items(), reverse=True)]


class VSlotBalancingStrategy(BalancingStrategy):
    def __init__(self, args, cluster_info, groups_info):
        super().__init__(args, cluster_info, groups_info)

    def order_candidate_vslots(self, candidate_vslots):
        return order_vslots_by_pdisk_usage(candidate_vslots, self.cluster_info.pdisk_usage)


class SpaceRatioBalancingStrategy(BalancingStrategy):
    def __init__(self, args, cluster_info, groups_info):
        super().__init__(args, cluster_info, groups_info)

    def order_candidate_vslots(self, candidate_vslots):
        pdisks = {
            pdisk_id: {
                "FreeSpaceRatio": float(pdisk.PDiskMetrics.AvailableSize) / float(pdisk.PDiskMetrics.TotalSize),
                "CandidateVSlots": [],
            }
            for pdisk_id, pdisk in self.cluster_info.pdisk_map.items()
            if pdisk.PDiskMetrics.TotalSize > 0  # pdisk works
        }
        for vslot in candidate_vslots:
            pdisk_id = common.get_pdisk_id(vslot.VSlotId)
            pdisks[pdisk_id]["CandidateVSlots"].append(vslot)
        return [info["CandidateVSlots"] for _, info in sorted(list(pdisks.items()), key=lambda x: x[1]["FreeSpaceRatio"])]


class GroupVSlotsBalancingStrategy(BalancingStrategy):
    def __init__(self, args, cluster_info, groups_info):
        super().__init__(args, cluster_info, groups_info)
        self.pdisk_groups_usage = None
        self.total_vdisks = 0
        self.ideal_distribution = None
        self.max_vdisks_per_pdisk = None
        self.min_vdisks_per_pdisk = None
        self.group_pdisks = None
        self.group_nodes = None
        self.pdisk_ids = None
        self.pdisk_type = None

    def calculate_extra_info(self):
        pdisk_types = set()
        for vslot in self.cluster_info.base_config.VSlot:
            if vslot.GroupId in self.args.group_ids:
                pdisk_id = common.get_pdisk_id(vslot.VSlotId)
                pdisk = self.cluster_info.pdisk_map[pdisk_id]
                pdisk_types.add((pdisk.BoxId, pdisk.Type))

        if len(pdisk_types) != 1:
            print(f"All pdisks must be of the same type, current types(boxId, type) {pdisk_types}", file=sys.stderr)
            return True

        self.pdisk_type = pdisk_types.pop()
        pdisk_box_id, pdisk_type = self.pdisk_type

        self.pdisk_ids = []
        for pdisk_id, pdisk in self.cluster_info.pdisk_map.items():
            if pdisk.BoxId == pdisk_box_id and pdisk.Type == pdisk_type and pdisk.DriveStatus == 1:
                self.pdisk_ids.append(pdisk_id)

        total_pdisks = len(self.pdisk_ids)

        self.pdisk_groups_usage = defaultdict(int)

        for pdisk_id in self.pdisk_ids:
            self.pdisk_groups_usage[pdisk_id] = 0

        vslots = filter_vslots_by_group_ids(self.cluster_info.base_config.VSlot, self.args.group_ids)

        for vslot in vslots:
            pdisk_id = common.get_pdisk_id(vslot.VSlotId)
            self.pdisk_groups_usage[pdisk_id] += 1

        self.total_vdisks = len(vslots)

        self.ideal_distribution = (self.total_vdisks + total_pdisks - 1) // total_pdisks

        self.max_vdisks_per_pdisk = max(self.pdisk_groups_usage.values())
        self.min_vdisks_per_pdisk = min(self.pdisk_groups_usage.values())

        self.group_pdisks = defaultdict(set)
        self.group_nodes = defaultdict(set)
        for vslot in vslots:
            self.group_pdisks[vslot.GroupId].add(common.get_pdisk_id(vslot.VSlotId))
            self.group_nodes[vslot.GroupId].add(vslot.VSlotId.NodeId)

        common.print_if_not_quiet(self.args, f"VSlots distribuition: {self.max_vdisks_per_pdisk}..{self.min_vdisks_per_pdisk} (ideal {self.ideal_distribution})", file=sys.stdout)
        distr = defaultdict(int)
        for pdisk_id, count in self.pdisk_groups_usage.items():
            distr[count] += 1
        distr_str = ', '.join(f'{k}: {v}' for k, v in sorted(distr.items()))
        common.print_if_verbose(self.args, f"PDisk usage distribution: {distr_str}", file=sys.stdout)

    def filter_must_first_vslots(self, candidate_vslots):
        filtered_vslots = []
        for vslot in candidate_vslots:
            pdisk_id = common.get_pdisk_id(vslot.VSlotId)
            if self.pdisk_groups_usage[pdisk_id] > self.ideal_distribution:
                filtered_vslots.append(vslot)
        if not filtered_vslots:
            return candidate_vslots
        return filtered_vslots

    def order_candidate_vslots(self, candidate_vslots):
        return order_vslots_by_pdisk_usage(candidate_vslots, self.pdisk_groups_usage)

    def check_success_conditions(self):
        if self.max_vdisks_per_pdisk <= self.ideal_distribution and self.min_vdisks_per_pdisk + 1 >= self.ideal_distribution:
            common.print_if_verbose(self.args, "VDisk distribution is close to ideal", file=sys.stdout)
            return True
        return False

    def _make_reassign_request(self, vslot, target_pdisk_id):
        request = common.create_bsc_request(self.args)
        cmd = request.Command.add().ReassignGroupDisk
        cmd.GroupId = vslot.GroupId
        cmd.GroupGeneration = vslot.GroupGeneration
        cmd.FailRealmIdx = vslot.FailRealmIdx
        cmd.FailDomainIdx = vslot.FailDomainIdx
        cmd.VDiskIdx = 0
        target = cmd.TargetPDiskId
        target.NodeId = target_pdisk_id[0]
        target.PDiskId = target_pdisk_id[1]
        return request

    def reassign_vslot(self, vslot, try_blocking):
        pdisk_id = common.get_pdisk_id(vslot.VSlotId)
        vslot_id = common.get_vslot_id(vslot.VSlotId)
        common.print_if_verbose(self.args, f"Reassigning vdisk {vslot_id} of group {vslot.GroupId} from pdisk {pdisk_id}", sys.stdout)

        current_pool_vdisks = self.pdisk_groups_usage[pdisk_id]
        ideal_vdisks = self.ideal_distribution

        if self.max_vdisks_per_pdisk > ideal_vdisks and current_pool_vdisks <= ideal_vdisks:
            common.print_if_verbose(
                self.args,
                f"Current pool vdisks {current_pool_vdisks} on pdisk {pdisk_id} is less than ideal {ideal_vdisks}",
                sys.stdout,
            )
            return False

        min_pool_vdisks = self.max_vdisks_per_pdisk
        target_pdisk = None

        for check_pdisk_id, current_count in self.pdisk_groups_usage.items():
            if check_pdisk_id == pdisk_id:
                continue

            if check_pdisk_id in self.group_pdisks[vslot.GroupId]:
                continue

            if check_pdisk_id[0] != pdisk_id[0] and not self.args.allow_same_node and check_pdisk_id[0] in self.group_nodes[vslot.GroupId]:
                continue

            check_pdisk = self.cluster_info.pdisk_map[check_pdisk_id]
            if check_pdisk.DriveStatus != 1:
                continue

            if current_count < min_pool_vdisks:
                min_pool_vdisks = current_count
                target_pdisk = check_pdisk_id

        if current_pool_vdisks <= min_pool_vdisks + 1:
            common.print_if_verbose(
                self.args,
                f"Current pool vdisks {current_pool_vdisks} on pdisk {pdisk_id} is not greater than min pool vdisks + 1 ({min_pool_vdisks + 1})",
                sys.stdout,
            )
            return False

        if not target_pdisk:
            common.print_if_verbose(
                self.args,
                f"Target pdisk {target_pdisk} not found",
                sys.stdout,
            )
            return False

        if min_pool_vdisks >= ideal_vdisks:
            common.print_if_verbose(
                self.args,
                f"Min pool vdisks {min_pool_vdisks} on pdisk {pdisk_id} is more than ideal {ideal_vdisks}",
                sys.stdout,
            )
            return False

        request = self._make_reassign_request(vslot, target_pdisk)

        response = common.invoke_bsc_request(request)
        if not common.is_successful_bsc_response(response):
            common.print_request_result(self.args, request, response)
            return False

        common.print_if_not_quiet(
            self.args,
            f"Reassigned vdisk {vslot_id} from pdisk {pdisk_id} ({current_pool_vdisks} vdisks) to pdisk {target_pdisk} ({min_pool_vdisks} vdisks)",
            sys.stdout,
        )
        return True


def balance_iteration(args, strategy, iteration_number):
    if strategy.calculate_extra_info():
        common.print_status(args, success=False, error_reason='Failed to calculate extra info')
        return False
    if strategy.verify_cluster_state():
        common.print_status(args, success=False, error_reason='Cluster state is not valid')
        return False
    if strategy.check_waiting_conditions():
        common.print_if_verbose(args, "Waiting for cluster state to become ready", file=sys.stdout)
        time.sleep(Constants.WAITING_TIME)
        return None
    if strategy.check_success_conditions():
        common.print_status(args, success=True, error_reason='')
        return True

    candidate_vslots, actually_no_vslots = strategy.list_candidate_vslots()
    if actually_no_vslots:
        common.print_status(args, success=True, error_reason='No vdisks suitable for relocation found')
        return True

    if not candidate_vslots:
        common.print_if_not_quiet(args, 'No vdisks suitable for relocation found, waiting..', sys.stdout)
        time.sleep(Constants.WAITING_TIME)
        return None

    vslots_ordered_groups_to_reassign = strategy.order_candidate_vslots(candidate_vslots)
    common.print_if_verbose(args, f"Found {len(candidate_vslots)} candidate vslots, {len(vslots_ordered_groups_to_reassign)} groups to reassign", file=sys.stdout)
    was_sent = False
    for vslots in vslots_ordered_groups_to_reassign:
        random.shuffle(vslots)
        for vslot in vslots:
            if strategy.reassign_vslot(vslot, False):
                was_sent = True
                break
        else:
            for vslot in vslots:
                if strategy.reassign_vslot(vslot, True):
                    was_sent = True
                    break
            else:
                continue
        break
    else:
        common.print_if_verbose(args, "No vdisks were sent for relocation", file=sys.stdout)
        common.print_status(args, success=True, error_reason='')
        return True
    if not was_sent:
        common.print_if_not_quiet(args, 'No relocation requests were successful, waiting..', sys.stdout)
        time.sleep(Constants.WAITING_TIME)
    elif Constants.TIME_BERWEEN_REASSIGNINGS > 0:
        time.sleep(Constants.TIME_BERWEEN_REASSIGNINGS)
    return None


def do(args):
    Constants.apply_args(args)
    if args.sort_by == 'slots':
        strategy_factory = VSlotBalancingStrategy
    elif args.sort_by == 'space_ratio':
        strategy_factory = SpaceRatioBalancingStrategy
    elif args.sort_by == 'group_slots':
        strategy_factory = GroupVSlotsBalancingStrategy
    else:
        print(f"Unknown sort by option: {args.sort_by}", file=sys.stderr)
        sys.exit(1)

    cluster_info = ClusterInfo.collect_cluster_info()
    existing_storage_pools = set(cluster_info.storage_pool_names_map.values())
    if args.storage_pool is not None and args.storage_pool not in existing_storage_pools:
        print(f"Storage pool {args.storage_pool} not found in existing storage pools: {list(sorted(existing_storage_pools))}")
        sys.exit(1)

    groups_info = GroupsInfo.collect_groups_info(cluster_info)
    if args.sort_by == 'group_slots':
        if args.group_ids is None and args.storage_pool is None:
            print("Group vslots balancing requires --group-ids or --storage-pool option", file=sys.stderr)
            sys.exit(1)

    if args.group_ids is not None and args.storage_pool is not None:
        group_ids = set(int(group_id) for group_id in args.group_ids)
        args.group_ids = set(id for id, pool_name in cluster_info.group_id_to_storage_pool_name_map.items() if pool_name == args.storage_pool and id in group_ids)
    elif args.group_ids is not None:
        args.group_ids = set(int(group_id) for group_id in args.group_ids)
    elif args.storage_pool is not None:
        args.group_ids = set(id for id, pool_name in cluster_info.group_id_to_storage_pool_name_map.items() if pool_name == args.storage_pool)

    iteration_number = 1
    while True:
        common.print_if_not_quiet(args, f"\nStart balancing iteration {iteration_number}", file=sys.stdout)
        if groups_info.unhealthy_groups:
            common.print_if_verbose(args, f'Skipping vdisks from unhealthy groups: {groups_info.unhealthy_groups}', file=sys.stdout)

        strategy = strategy_factory(args, cluster_info, groups_info)
        balancing_result = balance_iteration(args, strategy, iteration_number)
        if balancing_result is not None:
            break
        common.flush_cache()
        cluster_info = ClusterInfo.collect_cluster_info()
        groups_info = GroupsInfo.collect_groups_info(cluster_info)
        iteration_number += 1
    if balancing_result is False:
        sys.exit(1)
