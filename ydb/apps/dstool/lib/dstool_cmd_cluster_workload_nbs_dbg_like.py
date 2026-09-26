"""Thin argparse adapter; all workflow and recovery logic lives in nbs_dbg_like_load."""
import json
import math
from pathlib import Path
import sys
import uuid

from ydb.apps.dstool.lib import common, nbs_dbg_like_load as lib


description = 'Manage dedicated NbsDbgLike load tablets and recoverable gRPC runs'


def positive(value):
    import argparse
    number = float(value)
    if number <= 0 or not math.isfinite(number):
        raise argparse.ArgumentTypeError('must be positive and finite')
    return number


def add_options(parser):
    sub = parser.add_subparsers(dest='nbs_dbg_like_load_action', required=True)
    for action in ('create', 'list', 'describe', 'delete', 'run', 'results', 'stop'):
        p = sub.add_parser(action)
        p.add_argument('--database', help='Database containing the dedicated load tablets')
        p.add_argument('--node-id', type=int, metavar='ID', help='Coordinator node ID (default: receiving node)')
        p.add_argument('--rpc-timeout', type=positive, help='Per-RPC deadline in seconds (default: 90)')
        p.add_argument('--startup-timeout', type=int, help='Readiness and configuration budget in seconds (default: 60)')
        p.add_argument('--poll-interval', type=positive, help='Polling interval in seconds (default: 2)')
        p.add_argument('--wait-timeout', type=positive, help='Overall result wait in seconds')
        p.add_argument('--format', choices=('pretty', 'json', 'jsonl') if action == 'run' else ('pretty', 'json'),
                       default='pretty', help='Output format (default: pretty)')
        if action in ('create', 'describe', 'delete'):
            p.add_argument('--owner-index', type=int, required=True, help='Hive owner index of the dedicated tablet')
        if action in ('create', 'run'):
            defaults = action == 'create'
            p.add_argument('--pool-name', default='ddp1' if defaults else None,
                           help='DDisk and persistent-buffer pool for allocated tablet (default: ddp1)')
            p.add_argument('--ddisk-pool-name', help='Override the DDisk pool')
            p.add_argument('--pb-pool-name', help='Override the persistent-buffer pool')
            p.add_argument('--num-groups', type=int, default=32 if defaults else None,
                           help='Direct block groups for allocated tablet (default: 32)')
            p.add_argument('--target-num-vchunks', type=int, default=1 if defaults else None,
                           help='vChunks per group and allocation claim (default: 1)')
            p.add_argument('--vchunk-size-bytes', type=int, default=128 * 1024 * 1024 if defaults else None,
                           help='Bytes per vChunk (default: 134217728)')
            p.add_argument('--hosts-per-dbg', type=int, default=5 if defaults else None,
                           help='Expected DDisk/PB pairs per group, 3 to 5 (default: 5)')
            p.add_argument('--tablet-storage-pool', action='append', metavar='POOL',
                           help='Tablet channel pool; repeat in channel order if defaults are unavailable')
        if action == 'run':
            p.add_argument('--tablet-id', type=int,
                           help='Use an existing tablet; if no target is given, create one for this run')
            p.add_argument('--target', action='append', metavar='TABLET_ID[@NODE_ID]',
                           help='Repeat for multi-tablet runs; omitted node uses Hive placement, @0 uses coordinator')
            p.add_argument('--duration-seconds', type=int,
                           help='Duration of each run or sweep trial (default: 10 seconds)')
            p.add_argument('--delay-before-measurements-seconds', type=int,
                           help='Warmup duration in seconds (default: 0)')
            p.add_argument('--num-groups-to-use', type=int,
                           help='Allocated DBG prefix to use; 0 means all (default: 0)')
            p.add_argument('--read-ratio', type=int, help='Reads per 100 writes (default: 0)')
            p.add_argument('--sequential', action='store_true', help='Use sequential rather than random addresses')
            p.add_argument('--read-write-size-kib', type=int, help='I/O size in KiB (default: 4)')
            p.add_argument('--stop-on-writes-done-count', type=int,
                           help='Additional successful-write stopping limit (default: 0)')
            p.add_argument('--max-inflight-lsns', type=int, help='Tablet LSN cap (default: 65536)')
            p.add_argument('--flush-batch-size', type=int, help='Tablet flush batch size (default: 10000)')
            p.add_argument('--erase-batch-size', type=int, help='Tablet erase batch size (default: 10000)')
            p.add_argument('--sync-requests-batch-size', type=int,
                           help='Tablet sync request batch gate (default: 10)')
            p.add_argument('--pbuffer-reply-timeout-us', type=int,
                           help='Persistent-buffer reply timeout in microseconds (default: 50000)')
            p.add_argument('--disable-replication', action='store_true',
                           help='Write only to coordinator PB; incompatible with reads')
            p.add_argument('--disable-checksums', action='store_true', help='Disable per-block checksums')
            p.add_argument('--output-dir', help='Directory for checkpoints and results')
            p.add_argument('--resume', help='Resume from a saved artifact directory')
            p.add_argument('--allow-io-errors', action='store_true', default=None,
                           help='\nAllow even 100%% measured I/O errors in the CLI verdict')
            p.add_argument('--inflight', type=int, help='Single per-tablet I/O cap (default: 2048)')
            p.add_argument('--inflight-from', type=int, help='Start of doubling sweep')
            p.add_argument('--inflight-to', type=int, help='Inclusive upper bound of doubling sweep')
            p.add_argument('--trials', type=int, help='Odd positive trials per inflight value (default: 1)')
            p.add_argument('--no-wait', action='store_true', help='Return one explicit-tablet run handle after submission')
        if action in ('results', 'stop'):
            p.add_argument('--request-id', help='Client-generated run request ID')
            p.add_argument('--incarnation', help='Pinned coordinator service incarnation')
            p.add_argument('--handle', help='run artifact directory (single run only)')
            p.add_argument('--allow-io-errors', action='store_true', default=None,
                           help='\nAllow even 100%% measured I/O errors in the CLI verdict')
        if action == 'results':
            p.add_argument('--wait', action='store_true', help='Wait for a terminal result')


def tablet_readiness(tablet):
    summary = tablet.get('Summary', {})
    total = summary.get('NumDirectBlockGroups')
    if total is None:
        return 'unknown'
    ready = summary.get('NumReadyDirectBlockGroups', 0)
    return '%s/%s' % (ready, total)


def pretty(value, action=None, owner_index=None):
    if 'request' in value:
        return json.dumps(value, indent=2)
    if 'summary' in value:
        return '\n'.join('inflight=%s median trial=%s result=%s' % (row['inflight'], row['median_trial'], row['result'])
                         for row in value['summary'])
    if 'handle' in value:
        return 'Handle: %s\nArtifacts: %s' % (json.dumps(value['handle']), value['output_dir'])
    response = value.get('response', value)
    if 'Run' in response:
        run = response['Run']
        verdict = ('PENDING' if value.get('passed') is None else
                   'PASS' if value['passed'] else 'FAIL')
        lines = ['Workload execution: %s; drain confirmed=%s; measured=%s ms' % (
            run.get('State', 'IN_PROGRESS'), run.get('TerminationConfirmed', False),
            run.get('Stats', {}).get('MeasuredMs', '0')),
            'Measurement verdict: %s' % verdict]
        errors = value.get('measured_io_errors') or {}

        def error_text(direction):
            item = errors.get(direction, {})
            return '%s (%s)' % (item.get('count', '0'),
                                lib.format_error_percent(item.get('percent')))

        lines.append('write IOPS=%s write B/s=%s write errors=%s; read IOPS=%s read B/s=%s read errors=%s' % (
            run.get('WriteIOPS', 0), run.get('WriteBytesPerSecond', 0),
            error_text('writes'), run.get('ReadIOPS', 0),
            run.get('ReadBytesPerSecond', 0), error_text('reads')))
        for tablet in run.get('Tablets', []):
            lines.append('tablet=%s node=%s inflight=%s writes=%s/%s reads=%s/%s latency=%s' % (
                tablet.get('TabletId', '?'), tablet.get('NodeId', '?'), tablet.get('MaxInFlight', '?'),
                tablet.get('WritesOk', '0'), tablet.get('WritesErr', '0'),
                tablet.get('ReadsOk', '0'), tablet.get('ReadsErr', '0'),
                run.get('LatencyUnit', 'microseconds')))
        if value.get('failure_reason'):
            lines.append('Reason: %s' % value['failure_reason'])
        return '\n'.join(lines)
    if action == 'delete':
        return 'Delete completed for owner index %s in %s' % (owner_index, response['Database'])
    if action in ('list', 'create', 'describe'):
        tablets = response.get('Tablets', [])
        if not tablets:
            return 'No NbsDbgLike load tablets in %s' % response['Database']
        rows = [['OWNER INDEX', 'TABLET ID', 'NODE ID', 'READY DBGS', 'CHANNEL POOLS']]
        for tablet in tablets:
            rows.append([tablet.get('OwnerIndex', '?'), tablet.get('TabletId', '?'),
                         tablet.get('NodeId', '?'), tablet_readiness(tablet),
                         ', '.join(tablet.get('ChannelPools', [])) or '-'])
        widths = [max(len(str(row[index])) for row in rows) for index in range(len(rows[0]))]
        lines = ['  '.join(str(cell).ljust(widths[index]) for index, cell in enumerate(row)).rstrip()
                 for row in rows]
        if action in ('create', 'describe'):
            for tablet in tablets:
                summary = tablet.get('Summary', {})
                allocation = summary.get('Allocation', {})
                if allocation:
                    lines.append('Allocation for tablet %s:' % tablet.get('TabletId', '?'))
                    lines.extend('  %s: %s' % (key, ', '.join(map(str, item)) if isinstance(item, list) else item)
                                 for key, item in allocation.items())
                if summary.get('ErrorReason'):
                    lines.append('Tablet %s: %s' % (tablet.get('TabletId', '?'), summary['ErrorReason']))
        return '\n'.join(lines)
    return json.dumps(value, indent=2)


def pretty_sweep_trial(value, first, with_trial=False, show_tablets=True):
    run = value['response']['Run']
    stats = run.get('Stats', {})
    lines = []
    if first:
        tablets = run.get('Tablets', [])
        if tablets and show_tablets:
            lines.append('Tablets: ' + ', '.join('%s@%s' % (
                tablet.get('TabletId', '?'), tablet.get('NodeId', '?')) for tablet in tablets))
        lines.append('MaxInFlight  %sDirection      IOPS  p50 us  p95 us  p99 us  Errors  Error %%' %
                     ('Trial  ' if with_trial else ''))

    def row(direction, iops, histogram, error_direction):
        percentiles = lib.histogram_percentiles_us(histogram) or ('-', '-', '-')
        errors = (value.get('measured_io_errors') or {}).get(error_direction, {})
        return '%11s  %s%-9s  %8d  %6s  %6s  %6s  %6s  %7s' % (
            value['inflight'], ('%5s  ' % value['trial']) if with_trial else '',
            direction, int(float(iops or 0) + 0.5), *percentiles,
            errors.get('count', '0'), lib.format_error_percent(errors.get('percent')))

    lines.append(row('Writes', run.get('WriteIOPS', 0), stats.get('WriteLatencyUs', {}), 'writes'))
    read_ratio = (run.get('EffectiveConfig', {}).get('NbsDbgLikeLoad', {})
                  .get('WorkloadConfig', {}).get('ReadRatio', 0))
    if (read_ratio or run.get('ReadIOPS', 0) or int(stats.get('ReadsOk', 0))
            or int(stats.get('ReadsErr', 0))):
        lines.append(row('Reads', run.get('ReadIOPS', 0), stats.get('ReadLatencyUs', {}), 'reads'))
    if value.get('failure_reason'):
        lines.append('  inflight=%s%s: CLI check failed: %s' % (
            value['inflight'], (' trial=%s' % value['trial']) if with_trial else '',
            value['failure_reason']))
    return '\n'.join(lines)


def do(args):
    try:
        _do(args)
    except (lib.LoadError, ValueError, OSError) as error:
        # stdout remains either valid JSON/JSONL or human-oriented result output.
        print('cluster workload nbs-dbg-like: %s' % error, file=sys.stderr)
        raise SystemExit(1)


def _do(args):
    action = args.nbs_dbg_like_load_action
    values = []
    pretty_sweep = False
    printed_sweep_header = False
    sweep_trials = 1
    sweep_results = {}

    def emit(value):
        nonlocal printed_sweep_header
        if args.format == 'json':
            values.append(value)
        elif args.format == 'jsonl':
            print(json.dumps(value, sort_keys=True), flush=True)
        else:
            if pretty_sweep and 'inflight' in value and 'response' in value:
                sweep_results[value['result']] = value
                print(pretty_sweep_trial(value, not printed_sweep_header, sweep_trials > 1), flush=True)
                printed_sweep_header = True
            elif pretty_sweep and 'summary' in value:
                if sweep_trials > 1:
                    print('Median trials by write IOPS:', flush=True)
                    for index, row in enumerate(value['summary']):
                        print(pretty_sweep_trial(sweep_results[row['result']], index == 0, True, False), flush=True)
            else:
                print(pretty(value, action, getattr(args, 'owner_index', None)), flush=True)

    def flush():
        if args.format == 'json':
            print(json.dumps(values[0] if len(values) == 1 else values, sort_keys=True))

    if args.startup_timeout is not None and not 0 < args.startup_timeout <= lib.MAX_STARTUP_TIMEOUT:
        raise lib.LoadError('--startup-timeout must be positive and at most %d seconds' % lib.MAX_STARTUP_TIMEOUT)
    client = lib.Client(lib.GrpcTransport(common.connection_params, args.rpc_timeout or 90),
                        args.database, args.node_id or 0, poll=args.poll_interval or 2)
    if action == 'run':
        auto = None
        artifact_announced = False
        if args.resume:
            settings = ('tablet_id', 'target', 'duration_seconds', 'delay_before_measurements_seconds',
                        'num_groups_to_use', 'inflight', 'inflight_from', 'inflight_to', 'read_ratio', 'read_write_size_kib',
                        'stop_on_writes_done_count', 'max_inflight_lsns', 'flush_batch_size',
                        'erase_batch_size', 'sync_requests_batch_size', 'pbuffer_reply_timeout_us',
                        'output_dir', 'database', 'node_id', 'startup_timeout', 'poll_interval',
                        'wait_timeout', 'trials', 'rpc_timeout', 'pool_name', 'num_groups',
                        'target_num_vchunks', 'vchunk_size_bytes', 'hosts_per_dbg', 'tablet_storage_pool',
                        'ddisk_pool_name', 'pb_pool_name')
            flags = ('allow_io_errors', 'sequential', 'disable_replication', 'disable_checksums')
            if (any(getattr(args, name, None) is not None for name in settings)
                    or any(getattr(args, name, False) for name in flags)):
                raise lib.LoadError('--resume uses saved settings; new run settings are forbidden')
            if (Path(args.resume) / 'auto.json').exists():
                auto = lib.AutoLifecycle.resume(client, args.resume)
                if args.no_wait:
                    raise lib.LoadError('--no-wait requires one run on an explicit tablet')
            if not auto or (Path(args.resume) / 'checkpoint.json').exists():
                runner = lib.Runner.resume(client, args.resume, reconcile=not args.dry_run)
                if auto:
                    auto.check_runner(runner)
            else:
                runner = None
            if args.dry_run:
                emit({'checkpoint': runner.checkpoint if runner else auto.state})
                flush()
                return
            if runner is None:
                runner = auto.prepare_runner()
            elif any(entry['state'] != 'complete' for entry in runner.checkpoint['trials']):
                client.pin((lib.Control.START, lib.Control.GET, lib.Control.STOP))
            if args.no_wait and (auto or len(runner.checkpoint['trials']) != 1):
                raise lib.LoadError('--no-wait requires one run on an explicit tablet')
        else:
            if not args.database:
                raise lib.LoadError('--database is required')
            trials = args.trials if args.trials is not None else 1
            inflight = (2048 if args.inflight is None and args.inflight_from is None
                        and args.inflight_to is None else args.inflight)
            inflights = lib.inflight_values(inflight, args.inflight_from,
                                            args.inflight_to, trials)
            automatic = args.tablet_id is None and not args.target
            if args.no_wait and (automatic or len(inflights) * trials != 1):
                raise lib.LoadError('--no-wait requires one run on an explicit tablet')
            allocation_flags = ('pool_name', 'num_groups', 'target_num_vchunks',
                                'vchunk_size_bytes', 'hosts_per_dbg', 'tablet_storage_pool',
                                'ddisk_pool_name', 'pb_pool_name')
            if not automatic and any(getattr(args, flag) is not None for flag in allocation_flags):
                raise lib.LoadError('allocation options require an automatically created tablet')
            if args.read_ratio is not None and not 0 <= args.read_ratio <= 100:
                raise lib.LoadError('--read-ratio must be between 0 and 100')
            config = lib.make_run_config(
                tablet_id=1 if automatic else args.tablet_id, targets=args.target or (),
                duration_seconds=args.duration_seconds if args.duration_seconds is not None else 10,
                delay_before_measurements_seconds=(args.delay_before_measurements_seconds
                                                   if args.delay_before_measurements_seconds is not None else 0),
                num_groups_to_use=args.num_groups_to_use,
                max_inflight=inflights[0], read_ratio=args.read_ratio,
                sequential=args.sequential, read_write_size_kib=args.read_write_size_kib,
                stop_on_writes_done_count=args.stop_on_writes_done_count,
                max_inflight_lsns=args.max_inflight_lsns if args.max_inflight_lsns is not None else 65536,
                flush_batch_size=args.flush_batch_size,
                erase_batch_size=args.erase_batch_size,
                sync_requests_batch_size=args.sync_requests_batch_size,
                pbuffer_reply_timeout_us=args.pbuffer_reply_timeout_us,
                disable_replication=args.disable_replication,
                disable_checksums=args.disable_checksums)
            allocation = None
            if automatic:
                config.NbsDbgLikeLoad.ClearField('NbsDbgLikeTabletId')
                allocation = lib.make_allocation(args.pool_name if args.pool_name is not None else 'ddp1',
                                                 args.num_groups if args.num_groups is not None else 32,
                                                 args.target_num_vchunks if args.target_num_vchunks is not None else 1,
                                                 args.vchunk_size_bytes if args.vchunk_size_bytes is not None else 128 * 1024 * 1024,
                                                 args.hosts_per_dbg if args.hosts_per_dbg is not None else 5,
                                                 args.tablet_storage_pool or (), args.ddisk_pool_name,
                                                 args.pb_pool_name)
            if args.dry_run:
                if automatic:
                    emit({'create_request': lib.as_json(client.request(lib.Control.CREATE, Allocation=allocation)),
                          'run_template': lib.as_json(config), 'inflights': inflights, 'trials': trials})
                else:
                    emit({'request': lib.as_json(client.request(lib.Control.START, Load=config,
                          StartupTimeoutSeconds=args.startup_timeout or 60)), 'inflights': inflights,
                          'trials': trials})
                flush()
                return
            directory = args.output_dir or str(Path('nbs-dbg-like-load-results') / str(uuid.uuid4()))
            if automatic:
                client.pin((lib.Control.CREATE, lib.Control.DESCRIBE, lib.Control.START,
                            lib.Control.GET, lib.Control.STOP, lib.Control.DELETE))
                auto = lib.AutoLifecycle.create(client, directory, allocation, config,
                                                inflights, trials, args.startup_timeout or 60,
                                                bool(args.allow_io_errors), args.wait_timeout)
                print('Artifacts: %s' % auto.directory, file=sys.stderr)
                artifact_announced = True
                runner = auto.prepare_runner()
            else:
                client.pin((lib.Control.START, lib.Control.GET, lib.Control.STOP))
                runner = lib.Runner.create(client, directory, config, args.startup_timeout or 60,
                                           inflights, trials, bool(args.allow_io_errors),
                                           args.wait_timeout,
                                           'sweep' if len(inflights) * trials > 1 else 'run')
        if not artifact_announced:
            print('Artifacts: %s' % runner.directory, file=sys.stderr)
        pretty_sweep = runner.checkpoint['kind'] == 'sweep'
        sweep_trials = max(entry['trial'] for entry in runner.checkpoint['trials'])
        try:
            passed = runner.execute(emit, getattr(args, 'no_wait', False))
        finally:
            try:
                if auto:
                    if runner.safe_to_delete():
                        auto.cleanup(runner)
                    else:
                        print('Allocation retained; termination is unconfirmed.', file=sys.stderr)
            finally:
                flush()
        if not passed:
            raise SystemExit(1)
        return

    if action in ('results', 'stop'):
        runner = None
        if args.handle:
            if args.request_id or args.incarnation or args.database or args.node_id is not None:
                raise lib.LoadError('--handle conflicts with explicit run identity')
            runner = lib.Runner.from_handle(client, args.handle, reconcile=not args.dry_run)
            if args.rpc_timeout is not None:
                client.transport.timeout = args.rpc_timeout
            request = runner.trial_request()
            request_id = request.RequestId
        else:
            if not all((args.database, args.node_id, args.incarnation, args.request_id)):
                raise lib.LoadError('provide --handle or --database, --node-id, --incarnation and --request-id')
            client.incarnation = args.incarnation
            request_id = args.request_id
        request = client.request(lib.Control.STOP if action == 'stop' else lib.Control.GET, RequestId=request_id)
        if args.dry_run:
            emit({'request': lib.as_json(request)})
        else:
            response = runner.saved_response() if runner else None
            if response is None:
                client.pin((request.Operation,))
                try:
                    if action == 'stop':
                        response = client.stop(request_id, args.wait_timeout or 270)
                    elif args.wait:
                        timeout = args.wait_timeout or runner.checkpoint['wait_timeout'] if runner else args.wait_timeout
                        if not timeout:
                            timeout = (runner.checkpoint['startup'] +
                                       runner.trial_request().Load.NbsDbgLikeLoad.WorkloadConfig.DurationSeconds + 210
                                       if runner else 270)
                        deadline = client.clock() + timeout
                        current = client.get(request_id, deadline)
                        response = (current if current.Run.State in lib.TERMINAL else
                                    client.wait(request_id, timeout, deadline))
                    else:
                        response = client.get(request_id)
                except lib.LoadError as error:
                    if runner and isinstance(error, lib.WaitTimeout):
                        runner.checkpoint['trials'][0]['outcome'] = 'timed_out'
                        runner.save()
                    if (runner and error.response is not None and error.response.HasField('Run')
                            and error.response.Run.State in lib.TERMINAL):
                        runner.record_result(0, error.response)
                    raise
                except KeyboardInterrupt:
                    if runner:
                        runner.checkpoint['trials'][0]['outcome'] = 'interrupted'
                        runner.save()
                    raise
                if runner and response.Run.State in lib.TERMINAL:
                    runner.record_result(0, response)
            allow_errors = (args.allow_io_errors if args.allow_io_errors is not None else
                            runner.checkpoint['allow_io_errors'] if runner else False)
            outcome = runner.checkpoint['trials'][0].get('outcome') if runner else None
            result = lib.run_result_payload(response, allow_errors, outcome)
            emit(result)
            flush()
            if response.Run.State in lib.TERMINAL and not result['passed']:
                raise SystemExit(1)
            return
        flush()
        return

    if not args.database:
        raise lib.LoadError('--database is required')
    op = {'create': lib.Control.CREATE, 'list': lib.Control.LIST,
          'describe': lib.Control.DESCRIBE, 'delete': lib.Control.DELETE}[action]
    fields = {}
    if hasattr(args, 'owner_index'):
        if args.owner_index < 0:
            raise lib.LoadError('--owner-index must be nonnegative')
        fields['OwnerIndex'] = args.owner_index
    if action == 'create':
        fields['Allocation'] = lib.make_allocation(args.pool_name, args.num_groups,
                                                   args.target_num_vchunks, args.vchunk_size_bytes,
                                                   args.hosts_per_dbg, args.tablet_storage_pool or (),
                                                   args.ddisk_pool_name, args.pb_pool_name)
    if args.dry_run:
        emit({'request': lib.as_json(client.request(op, **fields))})
    else:
        client.pin((op,))
        response = client.call(client.request(op, **fields))
        if action == 'create':
            response = client.ready(args.owner_index, args.startup_timeout or 60)
        emit(lib.as_json(response))
    flush()
