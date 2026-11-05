from concurrent.futures import CancelledError, FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
from pathlib import Path
import sys
import threading
import time

from ydb.apps.dstool.lib import common


hidden = True

description = '''Evict ERROR VDisks of a storage pool in a DC, group by group.
For each group, sort matching current VDisks by logical position. Evict the first
three disks as a batch, repeat that batch, then evict each remaining disk once
in order, starting with the fourth. --inflight limits concurrent groups.
Skip groups with fewer than three matching disks.
Wait for all slots in a batch to change before starting the next batch.
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
    p.add_argument('--inflight', type=positive, default=1,
                   help='Maximum concurrently relocating groups (default: 1)')
    p.add_argument('--timeout', type=positive, default=300, help='Seconds to wait for each relocation')
    p.add_argument('--poll-interval', type=positive, default=5, help='Seconds between placement checks')
    p.add_argument('--state-dir', type=Path, help='New directory for per-batch evidence')
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
    return selected, [[0, 1, 2], [0, 1, 2]] + [[index] for index in range(3, len(selected))]


def current_disk(rows, group_id, wanted, pool):
    matches = [row for row in rows if row['GroupId'] == group_id and position(row) == wanted]
    if len(matches) != 1:
        raise common.QueryError('Expected one current VDisk at %s in group %d, found %d'
                                % (wanted, group_id, len(matches)))
    row = matches[0]
    if row['PoolName'] != pool:
        raise common.QueryError('Group %d no longer belongs to pool %r' % (group_id, pool))
    return row


def evict(args, rows):
    request = common.create_bsc_request(args)
    for row in rows:
        command = request.Command.add().ReassignGroupDisk
        for field in ('GroupId', 'GroupGeneration', 'FailRealmIdx', 'FailDomainIdx', 'VDiskIdx'):
            setattr(command, field, row[field])
    # BSC advances the generation on commit, so all commands use the snapshot's
    # generation. Keep it fixed across transport retries to avoid evicting twice.
    response = common.invoke_bsc_request(request)
    if not common.is_successful_bsc_response(response):
        raise common.QueryError('VDisk reassignment failed: %s' % response)


def write_evidence(directory, name, data):
    if directory is not None:
        (directory / name).write_text(json.dumps(data, indent=2), encoding='utf-8')


def check_stopped(stop):
    if stop.is_set():
        raise CancelledError()


def wait_for_move(args, group_id, wanted, before, step, stop):
    deadline = time.monotonic() + args.timeout
    while True:
        check_stopped(stop)
        rows, _ = fetch_disks()
        after = [current_disk(rows, group_id, key, args.pool) for key in wanted]
        if all(slot(new) != slot(old) for old, new in zip(before, after)):
            return rows, after
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise common.QueryError('Group %d, batch %d: not all slots changed; relocations may still be in progress'
                                    % (group_id, step))
        stop.wait(min(args.poll_interval, remaining))


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
    stop = threading.Event()

    def run_group(group_id):
        check_stopped(stop)
        print('Group %d' % group_id, flush=True)
        rows, dcs = (initial_disks, initial_dcs) if args.dry_run else fetch_disks()
        group_disks = [row for row in rows if row['GroupId'] == group_id and row['PoolName'] == args.pool]
        if not group_disks:
            print('Skipping group %d: no current VDisks' % group_id, flush=True)
            return False
        selected, order = make_plan(group_disks, dcs, group_id, args.dc)
        if not order:
            print('Skipping group %d: %d ERROR VDisks in %s; need at least 3'
                  % (group_id, len(selected), args.dc), flush=True)
            return False
        print('Order: ' + ' -> '.join(','.join(str(index + 1) for index in batch) for batch in order), flush=True)
        for index, row in enumerate(selected, 1):
            print('%d: %s slot=%s' % (index, row['VDiskId'], slot(row)), flush=True)
        if args.dry_run:
            for step, batch in enumerate(order, 1):
                ids = ['[%08x:_:%u:%u:%u]' % (group_id, *position(selected[index])) for index in batch]
                print('Batch %d: evict %s' % (step, ', '.join(ids)))
            print('Actual execution waits for all batch slots to change before the next batch.')
            return True
        group_dir = args.state_dir / ('group-%d' % group_id) if args.state_dir else None
        if group_dir:
            group_dir.mkdir()
        write_evidence(group_dir, 'plan.json', dict(selected=selected, order=order))
        # Reuse the final polling snapshot as the next batch's starting point.
        for step, batch in enumerate(order, 1):
            check_stopped(stop)
            wanted = [position(selected[index]) for index in batch]
            before = [current_disk(rows, group_id, key, args.pool) for key in wanted]
            print('Group %d, batch %d/%d, VDisks %s'
                  % (group_id, step, len(order), ','.join(str(index + 1) for index in batch)), flush=True)
            for row in before:
                print('  evict %s from %s' % (row['VDiskId'], slot(row)), flush=True)
            write_evidence(group_dir, '%d-before.json' % step, before)
            # Stop on an ambiguous result; an accepted eviction may still be in progress.
            check_stopped(stop)
            evict(args, before)
            rows, after = wait_for_move(args, group_id, wanted, before, step, stop)
            for row in after:
                print('Group %d: moved to %s, status=%s' % (group_id, slot(row), row['VSlotStatus']), flush=True)
            write_evidence(group_dir, '%d-after.json' % step, after)
        return True

    if args.dry_run:
        planned = sum(run_group(group_id) for group_id in group_ids)
        print('Planned: %d; skipped: %d; inflight: %d'
              % (planned, len(group_ids) - planned, args.inflight), flush=True)
        return

    def guarded_group(group_id):
        try:
            return run_group(group_id)
        except CancelledError:
            return None
        except BaseException:
            stop.set()
            raise

    completed = skipped = 0
    pending = iter(group_ids)
    active = set()
    with ThreadPoolExecutor(max_workers=args.inflight) as executor:
        try:
            while True:
                while len(active) < args.inflight and not stop.is_set():
                    group_id = next(pending, None)
                    if group_id is None:
                        break
                    active.add(executor.submit(guarded_group, group_id))
                if not active:
                    break
                done, active = wait(active, return_when=FIRST_COMPLETED)
                for future in done:
                    result = future.result()
                    if result is True:
                        completed += 1
                    elif result is False:
                        skipped += 1
                print('Completed: %d; skipped: %d; active: %d/%d'
                      % (completed, skipped, len(active), args.inflight), flush=True)
        finally:
            stop.set()
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
