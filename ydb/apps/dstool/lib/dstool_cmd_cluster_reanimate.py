import json
from pathlib import Path
import sys
import time

from ydb.apps.dstool.lib import common


description = '''Evict ERROR VDisks of a storage pool in a DC, group by group.
For each group, sort matching current VDisks by logical position. Evict the first
three disks, then evict the first and second again, then evict each remaining
disk once in order, starting with the fourth.
Skip groups with fewer than three matching disks.
Refresh the generation before each eviction and wait for the slot to change.
--dry-run only prints the plan and can be passed before or after the subcommand.
'''


def positive(value):
    number = int(value)
    if number <= 0:
        raise ValueError('Expected a positive integer')
    return number


def add_options(p):
    p.add_argument('--dry-run', '-n', dest='reanimate_dry_run', action='store_true',
                   help='Print the plan without evicting VDisks')
    p.add_argument('--pool', required=True, help='Exact storage pool name, including :media suffix')
    p.add_argument('--dc', required=True, help='Source data center')
    p.add_argument('--timeout', type=positive, default=300, help='Seconds to wait for each relocation')
    p.add_argument('--poll-interval', type=positive, default=5, help='Seconds between placement checks')
    p.add_argument('--state-dir', type=Path, help='New directory for per-step evidence')
    common.add_ignore_degraded_group_check_option(p)
    common.add_ignore_failure_model_group_check_option(p)


def position(row):
    return tuple(row[key] for key in ('FailRealmIdx', 'FailDomainIdx', 'VDiskIdx'))


def slot(row):
    return tuple(row[key] for key in ('NodeId', 'PDiskId', 'VSlotId'))


def fetch_disks():
    snapshot = common.fetch_base_config_and_storage_pools(cache=False)
    base = snapshot['BaseConfig']
    pools = {(pool.BoxId, pool.StoragePoolId): pool.Name for pool in snapshot['StoragePools']}
    vslots = common.build_vslot_map(base)
    rows = []
    for group in common.build_group_map(base).values():
        for slot_id in group.VSlotId:
            vslot = vslots.get(common.get_vslot_id(slot_id))
            if vslot is None:
                raise common.QueryError('Missing VSlot data for group %d' % group.GroupId)
            if vslot.GroupGeneration != group.GroupGeneration:
                continue
            rows.append(dict(
                PoolName=pools.get((group.BoxId, group.StoragePoolId)),
                GroupId=group.GroupId, GroupGeneration=vslot.GroupGeneration,
                NodeId=slot_id.NodeId, PDiskId=slot_id.PDiskId, VSlotId=slot_id.VSlotId,
                VDiskId='[%08x:%u:%u:%u:%u]' % (
                    vslot.GroupId, vslot.GroupGeneration, vslot.FailRealmIdx,
                    vslot.FailDomainIdx, vslot.VDiskIdx),
                VSlotStatus=vslot.Status,
                FailRealmIdx=vslot.FailRealmIdx, FailDomainIdx=vslot.FailDomainIdx, VDiskIdx=vslot.VDiskIdx,
            ))
    return rows, {node.NodeId: node.Location.DataCenter for node in base.Node}


def make_plan(rows, dcs, group_id, dc):
    selected = []
    for row in rows:
        if row['GroupId'] != group_id or row['VSlotStatus'].upper() != 'ERROR':
            continue
        if row['NodeId'] not in dcs:
            raise common.QueryError('Unknown DC for node %d' % row['NodeId'])
        if dcs[row['NodeId']] == dc:
            selected.append(row)
    selected.sort(key=position)
    if len(selected) < 3:
        return selected, []
    if len({position(row) for row in selected}) != len(selected):
        raise common.QueryError('Multiple current VDisks at the same logical position')
    return selected, [0, 1, 2, 0, 1] + list(range(3, len(selected)))


def current_disk(rows, group_id, wanted, pool):
    matches = [row for row in rows if row['GroupId'] == group_id and position(row) == wanted]
    if len(matches) != 1:
        raise common.QueryError('Expected one current VDisk at %s in group %d, found %d'
                                % (wanted, group_id, len(matches)))
    row = matches[0]
    if row['PoolName'] != pool:
        raise common.QueryError('Group %d no longer belongs to pool %r' % (group_id, pool))
    return row


def evict(args, row):
    request = common.create_bsc_request(args)
    command = request.Command.add().ReassignGroupDisk
    for field in ('GroupId', 'GroupGeneration', 'FailRealmIdx', 'FailDomainIdx', 'VDiskIdx'):
        setattr(command, field, row[field])
    # Keep the generation fixed across transport retries: do not evict a replacement twice.
    response = common.invoke_bsc_request(request)
    if not common.is_successful_bsc_response(response):
        raise common.QueryError('VDisk reassignment failed: %s' % response)


def write_evidence(directory, name, data):
    if directory is not None:
        (directory / name).write_text(json.dumps(data, indent=2), encoding='utf-8')


def wait_for_move(args, group_id, wanted, before, step):
    deadline = time.monotonic() + args.timeout
    while True:
        rows, _ = fetch_disks()
        after = current_disk(rows, group_id, wanted, args.pool)
        if slot(after) != slot(before):
            return after
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise common.QueryError('Step %d: slot did not change; relocation may still be in progress' % step)
        time.sleep(min(args.poll_interval, remaining))


def run(args):
    if not args.pool or not args.dc:
        raise common.QueryError('--pool and --dc must be nonempty')
    initial_disks, initial_dcs = fetch_disks()
    group_ids = sorted({row['GroupId'] for row in initial_disks if row['PoolName'] == args.pool})
    print('Pool %s: %d groups with current VDisks' % (args.pool, len(group_ids)), flush=True)
    if not group_ids:
        return
    if args.state_dir and not args.dry_run:
        args.state_dir.mkdir()
    skipped = 0
    completed = 0
    for group_id in group_ids:
        print('Group %d' % group_id, flush=True)
        rows, dcs = ((initial_disks, initial_dcs) if args.dry_run or group_id == group_ids[0]
                     else fetch_disks())
        group_disks = [row for row in rows if row['GroupId'] == group_id and row['PoolName'] == args.pool]
        if not group_disks:
            print('Skipping group %d: no current VDisks' % group_id, flush=True)
            skipped += 1
            continue
        selected, order = make_plan(group_disks, dcs, group_id, args.dc)
        if not order:
            print('Skipping group %d: %d ERROR VDisks in %s; need at least 3'
                  % (group_id, len(selected), args.dc), flush=True)
            skipped += 1
            continue
        print('Order: ' + ' -> '.join(str(index + 1) for index in order), flush=True)
        for index, row in enumerate(selected, 1):
            print('%d: %s slot=%s' % (index, row['VDiskId'], slot(row)), flush=True)
        if args.dry_run:
            for step, index in enumerate(order, 1):
                print('Step %d, VDisk %d: evict %s' % (step, index + 1, selected[index]['VDiskId']))
            print('Actual execution refreshes VDisk IDs before each step.')
            continue
        group_dir = args.state_dir / ('group-%d' % group_id) if args.state_dir else None
        if group_dir:
            group_dir.mkdir()
        write_evidence(group_dir, 'plan.json', dict(selected=selected, order=order))
        for step, index in enumerate(order, 1):
            wanted = position(selected[index])
            rows, _ = fetch_disks()
            before = current_disk(rows, group_id, wanted, args.pool)
            print('Step %d/%d, VDisk %d: evict %s from %s'
                  % (step, len(order), index + 1, before['VDiskId'], slot(before)), flush=True)
            write_evidence(group_dir, '%d-before.json' % step, before)
            # Stop on an ambiguous result; an accepted eviction may still be in progress.
            evict(args, before)
            after = wait_for_move(args, group_id, wanted, before, step)
            print('Moved to %s, status=%s' % (slot(after), after['VSlotStatus']), flush=True)
            write_evidence(group_dir, '%d-after.json' % step, after)
        completed += 1
    print('Completed: %d; skipped: %d; total: %d' % (completed, skipped, len(group_ids)), flush=True)


def do(args):
    args.dry_run = args.dry_run or getattr(args, 'reanimate_dry_run', False)
    try:
        run(args)
    except KeyboardInterrupt:
        print('Interrupted; accepted evicts may still be in progress.', file=sys.stderr)
        sys.exit(130)
    except OSError as error:
        raise common.QueryError('Cannot write reanimation evidence: %s' % error) from error
