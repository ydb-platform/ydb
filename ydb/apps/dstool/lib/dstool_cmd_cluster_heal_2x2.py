from collections import Counter
from pathlib import Path
import sys
import time

from ydb.apps.dstool.lib import common


description = '''Relocate one failed realm of mirror-3-dc groups in dead:2(3,3).
Select storage pools from a file (one name per line) and source hosts by --from.
BSC selects destination PDisks. Print each BSC request and validate it with
Rollback before applying it. Global --dry-run only performs this validation.
Each iteration checks the current number of READY VDisks: six means healed;
four or five means evict the remaining non-ready disks of the selected realm;
three means evict all three disks of that realm.
Reassignment preserves donor mode.
'''

WAITING_TIME = 15


def add_options(p):
    p.add_argument('--from', dest='from_prefix', required=True, help='Source hostname prefix')
    p.add_argument('--storage_pools_file', '--storage-pools-file', required=True,
                   help='UTF-8 file with one storage pool name per line; blank lines are ignored')
    common.add_allow_unusable_pdisks_option(p)
    p.add_argument('--move-only-to-operational-pdisks', action='store_true', help='Move VDisks only to operational PDisks')
    p.add_argument('--max-iterations', type=int, default=0,
                   help='Maximum number of healing iterations, including waits (0 = unlimited)')
    p.add_argument('--max-replicating-pdisks', type=int,
                   help='Wait while the number of replicating PDisks in the cluster exceeds this limit')
    p.add_argument('--time-between-reassignings', type=int, default=0,
                   help='Time in seconds to wait after successful reassignments')
    p.add_argument('--min-vdisk-uptime', type=int, default=0,
                   help='Skip groups with VDisk uptime below this number of seconds (0 = disabled)')


def read_pool_names(path):
    try:
        names = list(dict.fromkeys(line.strip() for line in Path(path).read_text(encoding='utf-8').splitlines()
                                   if line.strip()))
    except (OSError, UnicodeError) as error:
        raise common.QueryError('Cannot read storage pools file: %s' % error) from error
    if not names:
        raise common.QueryError('Storage pools file is empty')
    return names


def coordinates(vslot):
    return vslot.FailRealmIdx, vslot.FailDomainIdx, vslot.VDiskIdx


def select_sources(group, vslot_map, hosts, from_prefix):
    if not common.is_dynamic_group(group.GroupId) or group.ErasureSpecies != 'mirror-3-dc':
        return []
    slots = list(common.vslots_of_group(group, vslot_map))
    expected = {(realm, domain, 0) for realm in range(3) for domain in range(3)}
    if len(slots) != 9 or {coordinates(slot) for slot in slots} != expected:
        return []
    # Like viewer, count reported READY disks.
    alive = sum(slot.Status == 'READY' for slot in slots)
    if alive not in (3, 4, 5):
        return []
    ready_per_realm = [sum(slot.Status == 'READY' for slot in slots if slot.FailRealmIdx == realm)
                       for realm in range(3)]
    # Preserve the live realm and the other failed realm of this incident.
    if sorted(ready_per_realm) != [0, alive - 3, 3]:
        return []
    for realm in range(3):
        realm_slots = [slot for slot in slots if slot.FailRealmIdx == realm]
        sources = [slot for slot in realm_slots if slot.Status != 'READY']
        # Donors retain the source placement after BSC moves a disk anywhere.
        # READY disks need no further eviction and may have dropped their donors.
        if len(sources) == 6 - alive and all(
                any(hosts.get(vslot_id.NodeId, '').startswith(from_prefix)
                    for vslot_id in [slot.VSlotId] + [donor.VSlotId for donor in slot.Donors])
                for slot in sources):
            return sorted(sources, key=coordinates)
    return []


def uptime_wait(base, group, minimum):
    """Check current VDisk actors, including READY disks, without caching counters."""
    connected = {node.NodeId for node in base.Node if node.Connected}
    slots = list(common.vslots_of_group(group, common.build_vslot_map(base)))
    counters = {}
    for node_id in sorted({slot.VSlotId.NodeId for slot in slots} & connected):
        data = common.fetch('node/%d/counters/counters=vdisks/json' % node_id, cache=False)
        for sensor in data.get('sensors', []):
            labels = sensor.get('labels', {})
            if (labels.get('sensor') == 'SkeletonFrontUptimeSeconds'
                    and labels.get('subsystem') == 'timer' and labels.get('group') == str(group.GroupId)):
                # mirror-3-dc has three domains per realm and one disk per domain.
                key = node_id, int(labels['pdisk']), int(labels['orderNumber'])
                counters[key] = float(sensor['value'])
    wait = 0
    for slot in slots:
        if slot.VSlotId.NodeId not in connected:
            continue
        key = slot.VSlotId.NodeId, slot.VSlotId.PDiskId, slot.FailRealmIdx * 3 + slot.FailDomainIdx
        uptime = counters.get(key)
        if uptime is None:
            # A failed disk may have no actor. A running/starting disk with
            # missing telemetry must not be treated as old enough to evict.
            if slot.Status != 'ERROR':
                wait = max(wait, WAITING_TIME)
        else:
            wait = max(wait, minimum - uptime)
    return wait


def create_request(args, group, sources, rollback):
    request = common.create_bsc_request(args)
    request.Rollback = rollback
    request.IgnoreGroupFailModelChecks = True
    for slot in sources:
        cmd = request.Command.add().ReassignGroupDisk
        cmd.GroupId = group.GroupId
        # BSC spins the generation after processing the command list.
        cmd.GroupGeneration = group.GroupGeneration
        cmd.FailRealmIdx, cmd.FailDomainIdx, cmd.VDiskIdx = coordinates(slot)
    return request


def check_response(response, sources, rollback):
    if not (response.RollbackSuccess if rollback else response.Success):
        raise common.QueryError('BSC reassignment failed:\n%s' % response)
    if len(response.Status) != len(sources) or not all(status.Success for status in response.Status):
        raise common.QueryError('BSC did not validate all %d reassignments:\n%s' % (len(sources), response))
    expected = Counter(common.get_vslot_id(slot.VSlotId) for slot in sources)
    actual = Counter(common.get_vslot_id(item.From)
                     for status in response.Status for item in status.ReassignedItem)
    if actual != expected:
        raise common.QueryError('BSC proposed unexpected reassignments:\n%s' % response)


def ordered_groups(snapshot, names):
    base = snapshot['BaseConfig']
    pools = {(pool.BoxId, pool.StoragePoolId): pool for pool in snapshot['StoragePools']}
    pool_order = {name: index for index, name in enumerate(names)}
    candidates = []
    for group in common.build_group_map(base).values():
        pool = pools.get((group.BoxId, group.StoragePoolId))
        if pool is not None and pool.Name in pool_order:
            key = pool_order[pool.Name], group.GroupId
            candidates.append((key, pool, group))
    return sorted(candidates, key=lambda item: item[0])


def reassign_group(args, pool, group, sources):
    request = create_request(args, group, sources, rollback=True)
    response = common.invoke_bsc_request(request)
    check_response(response, sources, rollback=True)
    request.Rollback = args.dry_run
    print('# Group %d, pool %s, %d READY VDisks, evicting %d\n%s' % (
        group.GroupId, pool.Name, 6 - len(sources), len(sources), request), flush=True)
    if not args.dry_run:
        try:
            response = common.invoke_bsc_request(request)
            check_response(response, sources, rollback=False)
        except (common.QueryError, common.ConnectionError) as error:
            raise common.QueryError('Apply failed; inspect actual placement before retrying. %s' % error) from error


def do(args):
    if not args.from_prefix:
        raise common.QueryError('--from must be a nonempty hostname prefix')
    if args.max_iterations < 0:
        raise common.QueryError('--max-iterations must be nonnegative')
    if args.max_replicating_pdisks is not None and args.max_replicating_pdisks < 0:
        raise common.QueryError('--max-replicating-pdisks must be nonnegative')
    if args.time_between_reassignings < 0:
        raise common.QueryError('--time-between-reassignings must be nonnegative')
    if args.min_vdisk_uptime < 0:
        raise common.QueryError('--min-vdisk-uptime must be nonnegative')
    names = read_pool_names(args.storage_pools_file)
    snapshot = common.fetch_base_config_and_storage_pools(cache=False)
    missing = set(names) - {pool.Name for pool in snapshot['StoragePools']}
    if missing:
        raise common.QueryError('Unknown storage pools: %s' % ', '.join(sorted(missing)))
    submitted = 0
    skipped = 0
    iteration_number = 0
    # Dry-run does not change cluster state: validate each group only once.
    dry_run_validated = set()
    while args.max_iterations == 0 or iteration_number < args.max_iterations:
        # Refresh after both reassignment and waiting, including in dry-run.
        if iteration_number:
            snapshot = common.fetch_base_config_and_storage_pools(cache=False)
        iteration_number += 1
        base = snapshot['BaseConfig']
        vslots = common.build_vslot_map(base)
        hosts = common.build_node_fqdn_map(base)
        uptime_deadline = None
        replication_wait = False
        # Start from the first pool/group on every fresh snapshot, so a repeat
        # eviction takes priority as soon as the group's uptime permits it.
        for _, pool, group in ordered_groups(snapshot, names):
            group_id = group.GroupId
            if group_id in dry_run_validated:
                continue
            sources = select_sources(group, vslots, hosts, args.from_prefix)
            if not sources:
                continue
            if args.min_vdisk_uptime:
                try:
                    wait = uptime_wait(base, group, args.min_vdisk_uptime)
                except (common.QueryError, common.ConnectionError, KeyError, TypeError, ValueError) as error:
                    skipped += 1
                    print('Skipping group %d, pool %s: cannot read VDisk uptime: %s' % (
                        group_id, pool.Name, error), file=sys.stderr)
                    continue
                if wait > 0:
                    print('Skipping group %d, pool %s: VDisk uptime below %d seconds or unknown' % (
                        group_id, pool.Name, args.min_vdisk_uptime), file=sys.stderr)
                    deadline = time.monotonic() + wait
                    uptime_deadline = deadline if uptime_deadline is None else min(uptime_deadline, deadline)
                    continue
            initial = len(sources) == 3 and all(
                hosts.get(slot.VSlotId.NodeId, '').startswith(args.from_prefix) for slot in sources) and not any(
                hosts.get(donor.VSlotId.NodeId, '').startswith(args.from_prefix)
                for slot in sources for donor in slot.Donors)
            # The stalled disks of this group must not block their own recovery.
            if initial and args.max_replicating_pdisks is not None:
                # Match cluster balance: count each PDisk once, across all pools,
                # including starting/unknown slots but excluding READY and ERROR.
                replicating = {common.get_pdisk_id(slot.VSlotId) for slot in base.VSlot
                               if slot.Status not in ('READY', 'ERROR')}
                if len(replicating) > args.max_replicating_pdisks:
                    print('Waiting for replication: %d PDisks, limit %d (iteration %d)' % (
                        len(replicating), args.max_replicating_pdisks, iteration_number), file=sys.stderr)
                    replication_wait = True
                    continue
            try:
                reassign_group(args, pool, group, sources)
            except (common.QueryError, common.ConnectionError) as error:
                skipped += 1
                print('Skipping group %d, pool %s: %s' % (group_id, pool.Name, error), file=sys.stderr)
                continue
            submitted += 1
            if args.dry_run:
                dry_run_validated.add(group_id)
            if args.time_between_reassignings:
                time.sleep(args.time_between_reassignings)
            break
        else:
            # Sleep only if no group could be reassigned in this iteration.
            wait = WAITING_TIME if replication_wait else None
            if uptime_deadline is not None and not args.dry_run:
                wait = min(WAITING_TIME, max(0, uptime_deadline - time.monotonic()))
            if wait is None:
                break
            time.sleep(wait)
    if args.max_iterations and iteration_number >= args.max_iterations:
        print('Reached maximum number of iterations: %d' % args.max_iterations, file=sys.stderr)
    print('%d reassignment requests %s' % (submitted, 'validated (dry-run)' if args.dry_run else 'submitted'), file=sys.stderr)
    if skipped:
        print('%d unsuccessful group attempts skipped' % skipped, file=sys.stderr)
