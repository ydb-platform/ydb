import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ydb.apps.dstool.lib import nbs_dbg_like_load as lib
from ydb.core.protos import test_shard_control_pb2 as proto


class Clock:
    def __init__(self):
        self.now = 0

    def __call__(self):
        return self.now

    def sleep(self, duration):
        self.now += duration


def config():
    return lib.parse_config('NbsDbgLikeLoad { NbsDbgLikeTabletId: 42 WorkloadConfig { '
                            'DurationSeconds: 1 DelayBeforeMeasurementsSeconds: 0 } }', 'textproto')


class Server:
    def __init__(self):
        self.calls = []
        self.runs = {}
        self.io_errors = 0
        self.writes_ok = 100
        self.active = False

    def __call__(self, request, deadline=None):
        self.calls.append(request.Operation)
        response = proto.TNbsDbgLikeLoadControlResponse(
            Status=1, ProtocolVersion=1, Database='/Root', CoordinatorNodeId=2,
            Incarnation='inc', RequestId=request.RequestId)
        if request.Operation == lib.Control.CAPABILITIES:
            response.Operations.extend(range(8))
        elif request.Operation == lib.Control.START:
            self.runs[request.RequestId] = True
            response.Run.State = lib.Result.IN_PROGRESS
        elif request.Operation in (lib.Control.GET, lib.Control.STOP):
            if request.RequestId not in self.runs:
                raise lib.LoadError('unknown or expired')
            response.Run.State = lib.Result.IN_PROGRESS if self.active else lib.Result.SUCCEEDED
            response.Run.TerminationConfirmed = not self.active
            response.Run.Stats.WritesOk = self.writes_ok
            response.Run.Stats.WritesErr = self.io_errors
            response.Run.WriteIOPS = 100
            if request.Operation == lib.Control.STOP:
                self.active = False
        return response


class NbsDbgLikeLoadTest(unittest.TestCase):
    def client(self, server):
        clock = Clock()
        client = lib.Client(server, '/Root', poll=2, clock=clock, sleep=clock.sleep)
        client.pin((lib.Control.START,))
        return client

    def test_formats_defaults_and_bookkeeping(self):
        original = config()
        parsed = lib.parse_config(json.dumps(lib.as_json(original)), 'json')
        self.assertEqual(original, parsed)
        self.assertEqual(parsed.NbsDbgLikeLoad.WorkloadConfig.MaxInFlight, 32)
        for text in ('Stop {}', 'Tag: 1 NbsDbgLikeLoad {}',
                     'NbsDbgLikeLoad { RequireReady: true }'):
            with self.assertRaises(lib.LoadError):
                lib.parse_config(text, 'textproto')
        original.NbsDbgLikeLoad.WorkloadConfig.MaxInFlight = 0
        original.NbsDbgLikeLoad.WorkloadConfig.DurationSeconds = 30
        for fmt, data in (('json', json.dumps(lib.as_json(original))),
                          ('textproto', 'NbsDbgLikeLoad { NbsDbgLikeTabletId: 42 WorkloadConfig { '
                           'DurationSeconds: 30 MaxInFlight: 0 } }')):
            with self.assertRaisesRegex(lib.LoadError, 'MaxInFlight'):
                lib.parse_config(data, fmt)

    def test_run_flags_build_workload_and_multi_tablet_targets(self):
        request = lib.make_run_config(
            targets=['42', '43@0', '44@50000'], duration_seconds=60,
            delay_before_measurements_seconds=5, num_groups_to_use=2,
            max_inflight=8, read_ratio=25, sequential=True,
            read_write_size_kib=8, stop_on_writes_done_count=100,
            max_inflight_lsns=128, flush_batch_size=16, erase_batch_size=32,
            sync_requests_batch_size=1, pbuffer_reply_timeout_us=1000,
            disable_checksums=True)
        cmd = request.NbsDbgLikeLoad
        wc = cmd.WorkloadConfig
        self.assertEqual([(target.TabletId, target.HasField('NodeId'), target.NodeId)
                          for target in cmd.Targets],
                         [(42, False, 0), (43, True, 0), (44, True, 50000)])
        self.assertEqual(wc.DurationSeconds, 60)
        self.assertEqual(wc.DelayBeforeMeasurementsSeconds, 5)
        self.assertEqual(wc.NumDirectBlockGroupsToUse, 2)
        self.assertEqual(wc.MaxInFlight, 8)
        self.assertEqual(wc.ReadRatio, 25)
        self.assertTrue(wc.Sequential)
        self.assertEqual(wc.ReadWriteSizeKiB, 8)
        self.assertEqual(wc.StopOnWritesDoneCount, 100)
        self.assertEqual(wc.TabletConfig.MaxInflightLsns, 128)
        self.assertEqual(wc.TabletConfig.FlushBatchSize, 16)
        self.assertEqual(wc.TabletConfig.EraseBatchSize, 32)
        self.assertEqual(wc.TabletConfig.SyncRequestsBatchSize, 1)
        self.assertEqual(wc.TabletConfig.PBufferReplyTimeoutMicroseconds, 1000)
        self.assertFalse(wc.TabletConfig.EnableChecksums)
        defaults = lib.make_run_config(tablet_id=42, duration_seconds=60).NbsDbgLikeLoad.WorkloadConfig
        self.assertEqual(defaults.DelayBeforeMeasurementsSeconds, 15)
        self.assertEqual(defaults.MaxInFlight, 32)
        self.assertEqual(defaults.ReadWriteSizeKiB, 4)
        self.assertTrue(defaults.TabletConfig.EnableChecksums)
        for options in ({'tablet_id': 42, 'targets': ['43']},
                        {'targets': ['42', '42@1']}, {'targets': ['0']},
                        {'targets': ['42@4294967296']},
                        {'tablet_id': 42, 'duration_seconds': 15},
                        {'tablet_id': 42, 'duration_seconds': 60, 'disable_replication': True,
                         'read_ratio': 1}):
            with self.subTest(options=options), self.assertRaises(lib.LoadError):
                lib.make_run_config(**{'duration_seconds': 60, **options})

    def test_submitting_checkpoint_precedes_rpc_and_result_precedes_completion(self):
        server = Server()
        client = self.client(server)
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / 'run'
            runner = lib.Runner.create(client, directory, config())
            transport = client.transport

            def checked(request):
                if request.Operation == lib.Control.START:
                    saved = json.loads((directory / 'checkpoint.json').read_text())
                    self.assertEqual(saved['trials'][0]['state'], 'submitting')
                    self.assertEqual(saved['trials'][0]['request']['RequestId'], request.RequestId)
                return transport(request)

            client.transport = checked
            self.assertTrue(runner.execute(lambda _: None))
            saved = json.loads((directory / 'checkpoint.json').read_text())
            self.assertEqual(saved['trials'][0]['state'], 'complete')
            self.assertTrue((directory / saved['trials'][0]['result']).is_file())
            self.assertNotIn('SecurityToken', (directory / 'checkpoint.json').read_text())
            with self.assertRaises(FileExistsError):
                lib.Runner.create(client, directory, config())

    def test_unknown_submission_never_replaced(self):
        server = Server()
        client = self.client(server)
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / 'run'
            runner = lib.Runner.create(client, directory, config())
            runner.checkpoint['trials'][0]['state'] = 'submitting'
            runner.save()
            resumed = lib.Runner.resume(client, directory, 'run')
            with self.assertRaisesRegex(lib.LoadError, 'unknown'):
                resumed.execute(lambda _: None)
            self.assertNotIn(lib.Control.START, server.calls)

    def test_lost_reply_resume_recovers_original_without_start(self):
        server = Server()
        client = self.client(server)
        with tempfile.TemporaryDirectory() as root:
            runner = lib.Runner.create(client, Path(root) / 'run', config())
            request = runner.checkpoint['trials'][0]['request']
            server.runs[request['RequestId']] = True
            runner.checkpoint['trials'][0]['state'] = 'submitting'
            runner.save()
            self.assertTrue(lib.Runner.resume(client, runner.directory, 'run').execute(lambda _: None))
            self.assertNotIn(lib.Control.START, server.calls)

    def test_sweep_order_resume_and_fail_fast(self):
        server = Server()
        client = self.client(server)
        with tempfile.TemporaryDirectory() as root:
            runner = lib.Runner.create(client, Path(root) / 'run', config(), inflights=[8, 2], trials=2, kind='sweep')
            self.assertEqual([entry['inflight'] for entry in runner.checkpoint['trials']], [8, 8, 2, 2])
            server.io_errors = 1
            server.writes_ok = 0
            self.assertFalse(runner.execute(lambda _: None))
            self.assertEqual(server.calls.count(lib.Control.START), 1)
            self.assertFalse(lib.Runner.resume(client, runner.directory, 'sweep').execute(lambda _: None))
            self.assertEqual(server.calls.count(lib.Control.START), 1)

    def test_allow_errors_does_not_allow_execution_failure(self):
        response = proto.TNbsDbgLikeLoadControlResponse()
        response.Run.State = lib.Result.SUCCEEDED
        response.Run.Stats.WritesErr = 1
        response.Run.TerminationConfirmed = True
        self.assertFalse(lib.verdict(response))
        response.Run.Stats.WritesOk = 1
        self.assertTrue(lib.verdict(response))
        self.assertTrue(lib.verdict(response, True))
        response.Run.TerminationConfirmed = False
        self.assertFalse(lib.verdict(response, True))
        response.Run.State = lib.Result.FAILED
        self.assertFalse(lib.verdict(response, True))

    def test_measured_error_percentage_and_failure_reason(self):
        response = proto.TNbsDbgLikeLoadControlResponse()
        response.Run.State = lib.Result.SUCCEEDED
        response.Run.TerminationConfirmed = True
        response.Run.Stats.WritesOk = 90
        response.Run.Stats.WritesErr = 10
        response.Run.Stats.ReadsOk = 0
        result = lib.run_result_payload(response)
        self.assertTrue(result['passed'])
        self.assertEqual(result['measured_io_errors']['writes'],
                         {'count': '10', 'total': '100', 'percent': 10.0})
        self.assertEqual(result['measured_io_errors']['reads'],
                         {'count': '0', 'total': '0', 'percent': None})
        self.assertNotIn('failure_reason', result)
        from ydb.apps.dstool.lib import dstool_cmd_cluster_workload_nbs_dbg_like as command
        self.assertIn('write errors=10 (10.00%)', command.pretty(result))
        self.assertIn('Measurement verdict: PASS', command.pretty(result))
        response.Run.Stats.WritesOk = 0
        result = lib.run_result_payload(response)
        self.assertFalse(result['passed'])
        self.assertIn('100% measured I/O errors in writes', result['failure_reason'])
        self.assertIn('Measurement verdict: FAIL', command.pretty(result))
        self.assertTrue(lib.run_result_payload(response, allow_io_errors=True)['passed'])
        response.Run.Stats.WritesOk = 100000
        response.Run.Stats.WritesErr = 1
        self.assertTrue(lib.run_result_payload(response)['passed'])
        self.assertEqual(lib.format_error_percent(lib.measured_io_errors(response)['writes']['percent']), '<0.01%')
        response.Run.Stats.ReadsErr = 3
        result = lib.run_result_payload(response)
        self.assertFalse(result['passed'])
        self.assertIn('100% measured I/O errors in reads', result['failure_reason'])

    def test_timeout_cancels_and_does_not_start_next_trial(self):
        server = Server()
        server.active = True
        client = self.client(server)
        with tempfile.TemporaryDirectory() as root:
            runner = lib.Runner.create(client, Path(root) / 'run', config(), inflights=[1, 2], kind='sweep', wait_timeout=3)
            with self.assertRaisesRegex(lib.LoadError, 'cancellation completed'):
                runner.execute(lambda _: None)
            self.assertIn(lib.Control.STOP, server.calls)
            self.assertEqual(server.calls.count(lib.Control.START), 1)
            self.assertEqual(runner.checkpoint['trials'][0]['state'], 'complete')

    def test_incarnation_loss(self):
        client = self.client(Server())
        client.incarnation = 'old'
        with self.assertRaisesRegex(lib.LoadError, 'incarnation lost'):
            client.pin((lib.Control.GET,))

    def test_uint64_json_is_string(self):
        request = lib.Control(TabletId=2**63 + 1)
        self.assertEqual(lib.as_json(request)['TabletId'], str(2**63 + 1))

    def test_lifecycle_pretty_output(self):
        from ydb.apps.dstool.lib import dstool_cmd_cluster_workload_nbs_dbg_like as command
        response = proto.TNbsDbgLikeLoadControlResponse(Status=1, Database='/Root')
        self.assertEqual(command.pretty(lib.as_json(response), 'list'),
                         'No NbsDbgLike load tablets in /Root')
        tablet = response.Tablets.add()
        tablet.OwnerIndex = 7
        tablet.TabletId = 2**63 + 1
        tablet.NodeId = 50000
        tablet.ChannelPools.extend(['pool-a', 'pool-b'])
        tablet.Summary.NumDirectBlockGroups = 4
        tablet.Summary.NumReadyDirectBlockGroups = 3
        tablet.Summary.Allocation.DDiskPoolName = 'ddp1'
        listing = command.pretty(lib.as_json(response), 'list')
        self.assertIn('OWNER INDEX', listing)
        self.assertIn(str(2**63 + 1), listing)
        self.assertIn('3/4', listing)
        self.assertIn('pool-a, pool-b', listing)
        self.assertNotIn('"Tablets"', listing)
        description = command.pretty(lib.as_json(response), 'describe')
        self.assertIn('Allocation for tablet %s:' % (2**63 + 1), description)
        self.assertIn('DDiskPoolName: ddp1', description)
        self.assertEqual(command.pretty(lib.as_json(response), 'delete', 7),
                         'Delete completed for owner index 7 in /Root')

    def test_list_format_selects_pretty_or_json(self):
        from ydb.apps.dstool.lib import dstool_cmd_cluster_workload_nbs_dbg_like as command
        import argparse
        parser = argparse.ArgumentParser()
        command.add_options(parser)
        for fmt in ('pretty', 'json'):
            with self.subTest(fmt=fmt):
                args = parser.parse_args(['list', '--database', '/Root', '--format', fmt])
                args.dry_run = False
                output = io.StringIO()
                with contextlib.redirect_stdout(output), patch.object(lib.GrpcTransport, '__call__', side_effect=Server()):
                    command.do(args)
                if fmt == 'pretty':
                    self.assertEqual(output.getvalue().strip(), 'No NbsDbgLike load tablets in /Root')
                else:
                    self.assertEqual(json.loads(output.getvalue())['Database'], '/Root')

    def test_create_uses_allocation_flags_and_defaults(self):
        from ydb.apps.dstool.lib import dstool_cmd_cluster_workload_nbs_dbg_like as command
        from ydb.apps.dstool.lib.arg_parser import ArgumentParser
        parser = ArgumentParser()
        command.add_options(parser)
        output = io.StringIO()
        with contextlib.redirect_stdout(output), self.assertRaises(SystemExit) as exit_result:
            parser.parse_args(['create', '--help'])
        self.assertEqual(exit_result.exception.code, 0)
        self.assertIn('--num-groups', output.getvalue())
        self.assertIn('--target-num-vchunks', output.getvalue())
        self.assertNotIn('--config', output.getvalue())

        def dry_run(options):
            fresh_parser = ArgumentParser()
            command.add_options(fresh_parser)
            args = fresh_parser.parse_args(['create', '--database', '/Root', '--owner-index', '1',
                                            '--format', 'json', *options])
            args.dry_run = True
            output = io.StringIO()
            with contextlib.redirect_stdout(output), patch.object(
                    lib.GrpcTransport, '__call__', side_effect=AssertionError('network')):
                command.do(args)
            return json.loads(output.getvalue())['request']['Allocation']

        defaults = dry_run([])
        self.assertEqual(defaults['DDiskPoolName'], 'ddp1')
        self.assertEqual(defaults['PersistentBufferDDiskPoolName'], 'ddp1')
        self.assertEqual(defaults['NumDirectBlockGroups'], 32)
        self.assertEqual(defaults['TargetNumVChunks'], 1)
        self.assertEqual(defaults['VChunkSizeBytes'], '134217728')
        self.assertEqual(defaults['HostsPerDbg'], 5)
        self.assertNotIn('TabletId', defaults)
        selected = dry_run(['--pool-name', 'ddp2', '--num-groups', '4',
                            '--target-num-vchunks', '2', '--vchunk-size-bytes', '4096',
                            '--hosts-per-dbg', '3', '--tablet-storage-pool', 'tablet-a',
                            '--tablet-storage-pool', 'tablet-b'])
        self.assertEqual(selected['DDiskPoolName'], 'ddp2')
        self.assertEqual(selected['PersistentBufferDDiskPoolName'], 'ddp2')
        self.assertEqual(selected['TabletStoragePools'], ['tablet-a', 'tablet-b'])
        self.assertEqual(selected['NumDirectBlockGroups'], 4)
        self.assertEqual(selected['TargetNumVChunks'], 2)
        self.assertEqual(selected['VChunkSizeBytes'], '4096')
        self.assertEqual(selected['HostsPerDbg'], 3)
        for bad in ({'num_groups': 0}, {'target_num_vchunks': 0},
                    {'vchunk_size_bytes': 4097}, {'hosts_per_dbg': 2}):
            with self.subTest(bad=bad), self.assertRaisesRegex(lib.LoadError, 'geometry'):
                lib.make_allocation(**bad)

    def test_command_dry_run_no_transport_and_machine_stdout(self):
        from ydb.apps.dstool.lib import dstool_cmd_cluster_workload_nbs_dbg_like as command
        import argparse
        parser = argparse.ArgumentParser()
        command.add_options(parser)
        args = parser.parse_args(['run', '--database', '/Root', '--tablet-id', '42',
                                  '--duration-seconds', '30', '--inflight', '8', '--format', 'json'])
        args.dry_run = True
        out = io.StringIO()
        with contextlib.redirect_stdout(out), patch.object(lib.GrpcTransport, '__call__', side_effect=AssertionError('network')):
            command.do(args)
        result = json.loads(out.getvalue())
        self.assertIn('request', result)
        self.assertNotIn('response', result)
        self.assertEqual(result['request']['Load']['NbsDbgLikeLoad']['NbsDbgLikeTabletId'], '42')
        bad = parser.parse_args(['run', '--database', '/Root', '--tablet-id', '42',
                                 '--duration-seconds', '30', '--inflight', '0'])
        bad.dry_run = True
        with patch.object(lib.GrpcTransport, '__call__', side_effect=AssertionError('network')):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                command.do(bad)

    def test_run_and_doubling_sweep_flag_cli_with_custom_parser(self):
        from ydb.apps.dstool.lib import dstool_cmd_cluster_workload_nbs_dbg_like as command
        from ydb.apps.dstool.lib.arg_parser import ArgumentParser
        parser = ArgumentParser()
        command.add_options(parser)

        def dry_run(argv):
            fresh_parser = ArgumentParser()
            command.add_options(fresh_parser)
            args = fresh_parser.parse_args(argv)
            args.dry_run = True
            output = io.StringIO()
            with contextlib.redirect_stdout(output), patch.object(
                    lib.GrpcTransport, '__call__', side_effect=AssertionError('network')):
                command.do(args)
            return json.loads(output.getvalue())

        run = dry_run(['run', '--database', '/Root', '--target', '42', '--target', '43@0',
                       '--duration-seconds', '60', '--inflight', '8', '--read-ratio', '25', '--format', 'json'])
        cmd = run['request']['Load']['NbsDbgLikeLoad']
        self.assertEqual(cmd['Targets'], [{'TabletId': '42'}, {'TabletId': '43', 'NodeId': 0}])
        self.assertEqual(cmd['WorkloadConfig']['ReadRatio'], 25)
        defaults = dry_run(['run', '--database', '/Root', '--tablet-id', '42', '--format', 'json'])
        workload = defaults['request']['Load']['NbsDbgLikeLoad']['WorkloadConfig']
        self.assertEqual(workload['DurationSeconds'], 10)
        self.assertEqual(workload['DelayBeforeMeasurementsSeconds'], 0)
        self.assertEqual(workload['MaxInFlight'], 2048)
        self.assertEqual(workload['TabletConfig']['MaxInflightLsns'], 65536)
        sweep = dry_run(['run', '--database', '/Root', '--tablet-id', '42',
                         '--inflight-from', '2', '--inflight-to', '9', '--trials', '3',
                         '--format', 'json'])
        self.assertEqual(sweep['inflights'], [2, 4, 8])
        self.assertEqual(sweep['trials'], 3)
        self.assertEqual(sweep['request']['Load']['NbsDbgLikeLoad']['NbsDbgLikeTabletId'], '42')
        self.assertEqual(sweep['request']['Load']['NbsDbgLikeLoad']['WorkloadConfig']['DurationSeconds'], 10)

        automatic = dry_run(['run', '--database', '/Root', '--duration-seconds', '60',
                             '--inflight', '8', '--format', 'json'])
        self.assertEqual(automatic['create_request']['Allocation']['DDiskPoolName'], 'ddp1')
        self.assertNotIn('NbsDbgLikeTabletId', automatic['run_template']['NbsDbgLikeLoad'])
        self.assertNotIn('response', automatic)

        with tempfile.TemporaryDirectory() as root:
            saved = lib.Runner.create(self.client(Server()), Path(root) / 'run', config())
            resumed = dry_run(['run', '--resume', str(saved.directory), '--format', 'json'])
            self.assertEqual(resumed['checkpoint']['kind'], 'run')

    def test_inflight_range_matches_monitoring_page(self):
        self.assertEqual(lib.inflight_values(inflight_from=3, inflight_to=20, trials=3), [3, 6, 12])
        for options in ({'inflight_from': 0, 'inflight_to': 8},
                        {'inflight_from': 4, 'inflight_to': 2},
                        {'inflight': 4, 'inflight_from': 2, 'inflight_to': 8},
                        {'inflight': 4, 'trials': 2}):
            with self.assertRaises(lib.LoadError):
                lib.inflight_values(**options)

    def test_serialized_hdr_percentiles_match_bucket_resolution(self):
        self.assertEqual(lib.histogram_percentiles_us({
            'Lowest': '1', 'SignificantDigits': 2,
            'Values': ['10', '20', '1000'], 'Counts': ['5', '4', '1']}),
            (10, 1003, 1003))
        self.assertIsNone(lib.histogram_percentiles_us({'Values': [], 'Counts': []}))

    def test_automatic_run_creates_checkpoints_and_deletes_after_drain(self):
        from ydb.apps.dstool.lib import dstool_cmd_cluster_workload_nbs_dbg_like as command
        import argparse

        class AllocatedServer(Server):
            def __init__(self):
                super().__init__()
                self.owner = None
                self.deleted = False
                self.fail_get = False

            def __call__(self, request, deadline=None):
                if request.Operation == lib.Control.GET and self.fail_get:
                    raise lib.LoadError('unknown or expired run')
                response = super().__call__(request, deadline)
                if request.Operation == lib.Control.CREATE:
                    self.owner = request.OwnerIndex
                if request.Operation == lib.Control.DESCRIBE:
                    self.assert_owner(request.OwnerIndex)
                    tablet = response.Tablets.add()
                    tablet.OwnerIndex = self.owner
                    tablet.TabletId = 42
                    tablet.Summary.AutomationProtocolVersion = 1
                    tablet.Summary.NumDirectBlockGroups = 1
                    tablet.Summary.NumReadyDirectBlockGroups = 1
                if request.Operation == lib.Control.LIST and not self.deleted:
                    tablet = response.Tablets.add()
                    tablet.OwnerIndex = self.owner
                    tablet.TabletId = 42
                if request.Operation == lib.Control.DELETE:
                    self.assert_owner(request.OwnerIndex)
                    self.deleted = True
                return response

            def assert_owner(self, owner):
                assert owner == self.owner

        parser = argparse.ArgumentParser()
        command.add_options(parser)
        server = AllocatedServer()
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / 'auto'
            args = parser.parse_args(['run', '--database', '/Root', '--duration-seconds', '30',
                                      '--inflight', '4', '--output-dir', str(directory), '--format', 'json'])
            args.dry_run = False
            output = io.StringIO()
            diagnostic = io.StringIO()
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(diagnostic), patch.object(
                    lib.GrpcTransport, '__call__', side_effect=server):
                command.do(args)
            self.assertTrue(json.loads(output.getvalue())['passed'])
            self.assertEqual(diagnostic.getvalue().count('Artifacts:'), 1)
            self.assertTrue(server.deleted)
            self.assertEqual(json.loads((directory / 'auto.json').read_text())['state'], 'deleted')
            self.assertEqual(json.loads((directory / 'checkpoint.json').read_text())['trials'][0]['state'], 'complete')
            self.assertLess(server.calls.index(lib.Control.CREATE), server.calls.index(lib.Control.START))
            self.assertLess(server.calls.index(lib.Control.GET), server.calls.index(lib.Control.DELETE))

            interrupted = AllocatedServer()
            interrupted.fail_get = True
            recovery_dir = Path(root) / 'recovery'
            args = parser.parse_args(['run', '--database', '/Root', '--duration-seconds', '30',
                                      '--inflight', '4', '--output-dir', str(recovery_dir), '--format', 'json'])
            args.dry_run = False
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()), patch.object(
                    lib.GrpcTransport, '__call__', side_effect=interrupted), self.assertRaises(SystemExit):
                command.do(args)
            self.assertFalse(interrupted.deleted)
            self.assertEqual(json.loads((recovery_dir / 'auto.json').read_text())['state'], 'ready')
            interrupted.fail_get = False
            resume_args = parser.parse_args(['run', '--resume', str(recovery_dir), '--format', 'json'])
            resume_args.dry_run = False
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()), patch.object(
                    lib.GrpcTransport, '__call__', side_effect=interrupted):
                command.do(resume_args)
            self.assertTrue(interrupted.deleted)
            self.assertEqual(interrupted.calls.count(lib.Control.START), 1)

    def test_pretty_sweep_prints_common_tablet_once(self):
        from ydb.apps.dstool.lib import dstool_cmd_cluster_workload_nbs_dbg_like as command
        import argparse

        class TabletServer(Server):
            def __call__(self, request, deadline=None):
                response = super().__call__(request, deadline)
                if request.Operation == lib.Control.GET:
                    tablet = response.Run.Tablets.add()
                    tablet.TabletId = 42
                    tablet.NodeId = 2
                    response.Run.Stats.WriteLatencyUs.Lowest = 1
                    response.Run.Stats.WriteLatencyUs.SignificantDigits = 2
                    response.Run.Stats.WriteLatencyUs.Values.extend([10, 20, 1000])
                    response.Run.Stats.WriteLatencyUs.Counts.extend([5, 4, 1])
                    response.Run.Stats.ReadLatencyUs.Lowest = 1
                    response.Run.Stats.ReadLatencyUs.SignificantDigits = 2
                    response.Run.Stats.ReadLatencyUs.Values.extend([7, 14])
                    response.Run.Stats.ReadLatencyUs.Counts.extend([9, 1])
                    response.Run.Stats.ReadsOk = 10
                    response.Run.ReadIOPS = 1
                return response

        parser = argparse.ArgumentParser()
        command.add_options(parser)
        with tempfile.TemporaryDirectory() as root:
            args = parser.parse_args(['run', '--database', '/Root', '--tablet-id', '42',
                                      '--inflight-from', '128', '--inflight-to', '512',
                                      '--output-dir', str(Path(root) / 'sweep'), '--format', 'pretty'])
            args.dry_run = False
            output, diagnostic = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(diagnostic), patch.object(
                    lib.GrpcTransport, '__call__', side_effect=TabletServer()):
                command.do(args)
            self.assertEqual(output.getvalue().count('Tablets: 42@2'), 1)
            self.assertEqual(output.getvalue().count('MaxInFlight  Direction'), 1)
            writes = [line.split() for line in output.getvalue().splitlines() if 'Writes' in line]
            reads = [line.split() for line in output.getvalue().splitlines() if 'Reads' in line]
            self.assertEqual([row[0] for row in writes], ['128', '256', '512'])
            self.assertEqual([row[0] for row in reads], ['128', '256', '512'])
            self.assertEqual(writes[0][-5:-2], ['10', '1003', '1003'])
            self.assertEqual(writes[0][-2:], ['0', '0.00%'])
            self.assertEqual(reads[0][-5:-2], ['7', '14', '14'])
            self.assertEqual(reads[0][-2:], ['0', '0.00%'])
            self.assertNotIn('tablet=42', output.getvalue())
            self.assertEqual(diagnostic.getvalue().count('Artifacts:'), 1)

            args = parser.parse_args(['run', '--database', '/Root', '--tablet-id', '42',
                                      '--inflight-from', '128', '--inflight-to', '256', '--trials', '3',
                                      '--output-dir', str(Path(root) / 'trials'), '--format', 'pretty'])
            args.dry_run = False
            output = io.StringIO()
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()), patch.object(
                    lib.GrpcTransport, '__call__', side_effect=TabletServer()):
                command.do(args)
            self.assertEqual(output.getvalue().count('Tablets: 42@2'), 1)
            self.assertEqual(output.getvalue().count('MaxInFlight  Trial  Direction'), 2)
            self.assertEqual(output.getvalue().count('Median trials by write IOPS:'), 1)
            self.assertEqual(output.getvalue().count('Writes'), 8)

            partial = TabletServer()
            partial.io_errors = 1
            args = parser.parse_args(['run', '--database', '/Root', '--tablet-id', '42',
                                      '--inflight-from', '128', '--inflight-to', '256',
                                      '--output-dir', str(Path(root) / 'partial'), '--format', 'pretty'])
            args.dry_run = False
            output = io.StringIO()
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()), patch.object(
                    lib.GrpcTransport, '__call__', side_effect=partial):
                command.do(args)
            self.assertEqual(output.getvalue().count('0.99%'), 2)
            self.assertNotIn('CLI check failed', output.getvalue())
            self.assertEqual(partial.calls.count(lib.Control.START), 2)
            partial_checkpoint = json.loads((Path(root) / 'partial' / 'checkpoint.json').read_text())
            partial_request_id = partial_checkpoint['trials'][0]['request']['RequestId']
            partial_args = parser.parse_args(['results', '--database', '/Root', '--node-id', '2',
                                              '--incarnation', 'inc', '--request-id', partial_request_id,
                                              '--format', 'json'])
            partial_args.dry_run = False
            partial_output = io.StringIO()
            with contextlib.redirect_stdout(partial_output), contextlib.redirect_stderr(io.StringIO()), patch.object(
                    lib.GrpcTransport, '__call__', side_effect=partial):
                command.do(partial_args)
            partial_result = json.loads(partial_output.getvalue())
            self.assertTrue(partial_result['passed'])
            self.assertAlmostEqual(partial_result['measured_io_errors']['writes']['percent'], 100 / 101)
            self.assertNotIn('failure_reason', partial_result)

            failed = TabletServer()
            failed.io_errors = 1
            failed.writes_ok = 0
            args = parser.parse_args(['run', '--database', '/Root', '--tablet-id', '42',
                                      '--inflight-from', '128', '--inflight-to', '256',
                                      '--output-dir', str(Path(root) / 'failed'), '--format', 'pretty'])
            args.dry_run = False
            output = io.StringIO()
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()), patch.object(
                    lib.GrpcTransport, '__call__', side_effect=failed), self.assertRaises(SystemExit):
                command.do(args)
            failed_write = next(line.split() for line in output.getvalue().splitlines() if 'Writes' in line)
            self.assertEqual(failed_write[-2:], ['1', '100.00%'])
            self.assertIn('inflight=128: CLI check failed:', output.getvalue())
            self.assertIn('100% measured I/O errors in writes', output.getvalue())

            checkpoint = json.loads((Path(root) / 'failed' / 'checkpoint.json').read_text())
            request_id = checkpoint['trials'][0]['request']['RequestId']
            result_args = parser.parse_args(['results', '--database', '/Root', '--node-id', '2',
                                             '--incarnation', 'inc', '--request-id', request_id,
                                             '--format', 'json'])
            result_args.dry_run = False
            result_output = io.StringIO()
            with contextlib.redirect_stdout(result_output), contextlib.redirect_stderr(io.StringIO()):
                with patch.object(lib.GrpcTransport, '__call__', side_effect=failed), self.assertRaises(SystemExit):
                    command.do(result_args)
            result = json.loads(result_output.getvalue())
            self.assertEqual(result['measured_io_errors']['writes']['count'], '1')
            self.assertEqual(result['measured_io_errors']['writes']['total'], '1')
            self.assertEqual(result['measured_io_errors']['writes']['percent'], 100.0)
            self.assertIn('100% measured I/O errors', result['failure_reason'])

            jsonl_args = parser.parse_args(['run', '--database', '/Root', '--tablet-id', '42',
                                            '--inflight-from', '128', '--inflight-to', '256',
                                            '--output-dir', str(Path(root) / 'failed-jsonl'), '--format', 'jsonl'])
            jsonl_args.dry_run = False
            jsonl_output = io.StringIO()
            with contextlib.redirect_stdout(jsonl_output), contextlib.redirect_stderr(io.StringIO()):
                with patch.object(lib.GrpcTransport, '__call__', side_effect=failed), self.assertRaises(SystemExit):
                    command.do(jsonl_args)
            trial = json.loads(jsonl_output.getvalue().strip())
            self.assertEqual(trial['inflight'], 128)
            self.assertEqual(trial['measured_io_errors']['writes']['percent'], 100.0)
            self.assertIn('100% measured I/O errors', trial['failure_reason'])

    def test_transport_uses_legacy_grpc_with_deadline_and_redacts_failure(self):
        import grpc
        from types import SimpleNamespace
        from ydb.core.protos import grpc_pb2_grpc
        endpoint = SimpleNamespace(host_with_grpc_port='example:2135', protocol='grpc')
        params = SimpleNamespace(grpc_endpoints={'a': endpoint}, token='secret')
        response = __import__('ydb.core.protos.msgbus_pb2', fromlist=['TResponse']).TResponse(Status=1)
        response.NbsDbgLikeLoadControl.Status = 1
        captured = []

        def rpc(request, timeout):
            captured.append((request, timeout))
            return response

        with patch.object(grpc, 'insecure_channel'), patch.object(grpc_pb2_grpc, 'TGRpcServerStub') as stub:
            stub.return_value.TestShardControl.side_effect = rpc
            lib.GrpcTransport(params, timeout=17)(lib.Control(Operation=lib.Control.GET))
        self.assertEqual(captured[0][0].SecurityToken, 'secret')
        self.assertGreater(captured[0][1], 0)
        self.assertLessEqual(captured[0][1], 17)
        response.ClearField('NbsDbgLikeLoadControl')
        response.Status = 128
        response.ErrorReason = 'rejected secret'
        with patch.object(grpc, 'insecure_channel'), patch.object(grpc_pb2_grpc, 'TGRpcServerStub') as stub:
            stub.return_value.TestShardControl.side_effect = rpc
            with self.assertRaisesRegex(lib.LoadError, '<redacted>') as caught:
                lib.GrpcTransport(params)(lib.Control(Operation=lib.Control.GET))
            self.assertNotIn('secret', str(caught.exception))

    def test_saved_result_recovers_crash_before_checkpoint_complete(self):
        server = Server()
        client = self.client(server)
        with tempfile.TemporaryDirectory() as root:
            runner = lib.Runner.create(client, Path(root) / 'run', config())
            self.assertTrue(runner.execute(lambda _: None))
            runner.checkpoint['trials'][0]['state'] = 'accepted'
            del runner.checkpoint['trials'][0]['result']
            runner.save()
            server.runs.clear()  # coordinator history can disappear after the fsync
            calls_before = len(server.calls)
            resumed = lib.Runner.resume(client, runner.directory, 'run')
            self.assertTrue(resumed.execute(lambda _: None))
            self.assertEqual(len(server.calls), calls_before)

    def test_handle_reconciles_and_serves_terminal_result_offline(self):
        server = Server()
        client = self.client(server)
        with tempfile.TemporaryDirectory() as root:
            runner = lib.Runner.create(client, Path(root) / 'run', config())
            self.assertTrue(runner.execute(lambda _: None))
            runner.checkpoint['trials'][0]['state'] = 'accepted'
            runner.checkpoint['trials'][0].pop('result')
            runner.save()
            offline = lib.Runner.from_handle(client, runner.directory)
            self.assertEqual(offline.checkpoint['trials'][0]['state'], 'complete')
            self.assertEqual(offline.saved_response().Run.State, lib.Result.SUCCEEDED)
            count = len(server.calls)
            self.assertTrue(offline.execute(lambda _: None))
            self.assertEqual(len(server.calls), count)
            offline.checkpoint['trials'][0]['state'] = 'accepted'
            offline.save()
            before = (offline.directory / 'checkpoint.json').read_bytes()
            lib.Runner.from_handle(client, offline.directory, reconcile=False)
            self.assertEqual((offline.directory / 'checkpoint.json').read_bytes(), before)

    def test_handle_results_persist_then_stop_reads_offline(self):
        import argparse
        from ydb.apps.dstool.lib import dstool_cmd_cluster_workload_nbs_dbg_like as command
        server = Server()
        client = self.client(server)
        parser = argparse.ArgumentParser()
        command.add_options(parser)
        with tempfile.TemporaryDirectory() as root:
            runner = lib.Runner.create(client, Path(root) / 'run', config())
            entry = runner.checkpoint['trials'][0]
            entry['state'] = 'accepted'
            runner.save()
            server.runs[entry['request']['RequestId']] = True
            args = parser.parse_args(['results', '--handle', str(runner.directory), '--format', 'json'])
            args.dry_run = False
            with patch.object(lib.GrpcTransport, '__call__', side_effect=server), contextlib.redirect_stdout(io.StringIO()):
                command.do(args)
            self.assertEqual(json.loads((runner.directory / 'checkpoint.json').read_text())['trials'][0]['state'], 'complete')
            self.assertTrue((runner.directory / 'result-0000.json').exists())
            args = parser.parse_args(['stop', '--handle', str(runner.directory), '--format', 'json'])
            args.dry_run = False
            with patch.object(lib.GrpcTransport, '__call__', side_effect=AssertionError('network')):
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    command.do(args)
            self.assertTrue(json.loads(out.getvalue())['passed'])

    def test_handle_interruption_keeps_failed_verdict(self):
        import argparse
        from ydb.apps.dstool.lib import dstool_cmd_cluster_workload_nbs_dbg_like as command
        server = Server()
        client = self.client(server)
        parser = argparse.ArgumentParser()
        command.add_options(parser)
        with tempfile.TemporaryDirectory() as root:
            runner = lib.Runner.create(client, Path(root) / 'run', config())
            entry = runner.checkpoint['trials'][0]
            entry['state'] = 'accepted'
            runner.save()
            server.runs[entry['request']['RequestId']] = True
            args = parser.parse_args(['results', '--handle', str(runner.directory), '--format', 'json'])
            args.dry_run = False

            def interrupted(request, deadline=None):
                if request.Operation == lib.Control.GET:
                    raise KeyboardInterrupt()
                return server(request)

            with patch.object(lib.GrpcTransport, '__call__', side_effect=interrupted):
                with self.assertRaises(KeyboardInterrupt):
                    command.do(args)
            self.assertEqual(json.loads((runner.directory / 'checkpoint.json').read_text())
                             ['trials'][0]['outcome'], 'interrupted')
            with patch.object(lib.GrpcTransport, '__call__', side_effect=server):
                out = io.StringIO()
                with contextlib.redirect_stdout(out), self.assertRaises(SystemExit):
                    command.do(args)
            self.assertFalse(json.loads(out.getvalue())['passed'])

    def test_saved_result_identity_mismatch_is_rejected(self):
        client = self.client(Server())
        with tempfile.TemporaryDirectory() as root:
            runner = lib.Runner.create(client, Path(root) / 'run', config())
            wrong = proto.TNbsDbgLikeLoadControlResponse(
                Database='/Root', CoordinatorNodeId=2, Incarnation='inc', RequestId='wrong')
            wrong.Run.State = lib.Result.SUCCEEDED
            lib.atomic_json(runner.directory / 'result-0000.json', lib.as_json(wrong))
            with self.assertRaisesRegex(lib.LoadError, 'identity'):
                lib.Runner.from_handle(client, runner.directory)

    def test_deadline_checks_late_terminal_and_caps_rpc_timeout(self):
        clock = Clock()
        observed = []

        def late(request):
            observed.append(request.RpcTimeoutMs)
            clock.now += 3
            response = proto.TNbsDbgLikeLoadControlResponse(
                Status=1, Database='/Root', CoordinatorNodeId=2, Incarnation='inc', RequestId=request.RequestId)
            response.Run.State = lib.Result.SUCCEEDED
            response.Run.TerminationConfirmed = True
            return response
        client = lib.Client(late, '/Root', 2, 'inc', clock=clock, sleep=clock.sleep)
        with self.assertRaises(lib.WaitTimeout) as caught:
            client.get('run', clock() + 2)
        self.assertEqual(observed, [2000])
        self.assertEqual(caught.exception.response.Run.State, lib.Result.SUCCEEDED)

    def test_late_success_keeps_timeout_verdict_after_resume(self):
        server = Server()
        client = self.client(server)
        clock = client.clock
        original = client.transport

        def late(request):
            response = original(request)
            if request.Operation == lib.Control.GET:
                clock.now += 4
            return response

        client.transport = late
        with tempfile.TemporaryDirectory() as root:
            runner = lib.Runner.create(client, Path(root) / 'run', config(),
                                       inflights=[1, 2], kind='sweep', wait_timeout=3)
            with self.assertRaisesRegex(lib.LoadError, 'timed out'):
                runner.execute(lambda _: None)
            self.assertEqual(runner.checkpoint['trials'][0]['outcome'], 'timed_out')
            self.assertEqual(runner.checkpoint['trials'][0]['state'], 'complete')
            starts = server.calls.count(lib.Control.START)
            resumed = lib.Runner.resume(client, runner.directory, 'sweep')
            self.assertFalse(resumed.execute(lambda _: None))
            self.assertEqual(server.calls.count(lib.Control.START), starts)

    def test_gateway_control_deadline_is_wait_timeout(self):
        import grpc
        from types import SimpleNamespace
        from ydb.core.protos import grpc_pb2_grpc, msgbus_pb2
        endpoint = SimpleNamespace(host_with_grpc_port='example:2135', protocol='grpc')
        params = SimpleNamespace(grpc_endpoints={'a': endpoint}, token=None)
        reply = msgbus_pb2.TResponse(
            Status=128, ErrorReason='control deadline exceeded; outcome unknown, retry the same request ID')
        with patch.object(grpc, 'insecure_channel'), patch.object(grpc_pb2_grpc, 'TGRpcServerStub') as stub:
            stub.return_value.TestShardControl.return_value = reply
            with self.assertRaises(lib.WaitTimeout):
                lib.GrpcTransport(params)(lib.Control(Operation=lib.Control.START))

    def test_transport_retries_share_one_deadline(self):
        import grpc
        from types import SimpleNamespace
        from ydb.core.protos import grpc_pb2_grpc, msgbus_pb2
        clock = Clock()

        class LostReply(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE
        endpoints = {name: SimpleNamespace(host_with_grpc_port=name, protocol='grpc')
                     for name in ('a', 'b')}
        params = SimpleNamespace(grpc_endpoints=endpoints, token=None)
        reply = msgbus_pb2.TResponse(Status=1)
        reply.NbsDbgLikeLoadControl.Status = 1
        timeouts = []

        def rpc(request, timeout):
            timeouts.append((timeout, request.NbsDbgLikeLoadControl.RpcTimeoutMs))
            if len(timeouts) == 1:
                clock.now += 4
                raise LostReply()
            return reply
        with patch.object(grpc, 'insecure_channel'), patch.object(grpc_pb2_grpc, 'TGRpcServerStub') as stub:
            stub.return_value.TestShardControl.side_effect = rpc
            lib.GrpcTransport(params, timeout=10, clock=clock)(lib.Control(Operation=lib.Control.GET))
        self.assertEqual([round(value[0]) for value in timeouts], [10, 6])
        self.assertEqual([value[1] for value in timeouts], [10000, 6000])

    def test_no_wait_terminal_failure_is_saved_and_fails(self):
        server = Server()
        client = self.client(server)
        original = client.transport

        def transport(request):
            response = original(request)
            if request.Operation == lib.Control.START:
                response.Run.State = lib.Result.FAILED
                response.Run.ExecutionError = 'startup failed'
            return response

        client.transport = transport
        with tempfile.TemporaryDirectory() as root:
            runner = lib.Runner.create(client, Path(root) / 'run', config())
            self.assertFalse(runner.execute(lambda _: None, no_wait=True))
            self.assertEqual(runner.checkpoint['trials'][0]['state'], 'complete')

    def test_sweep_resumes_pending_trials_and_selects_median(self):
        server = Server()
        client = self.client(server)
        with tempfile.TemporaryDirectory() as root:
            runner = lib.Runner.create(client, Path(root) / 'run', config(), inflights=[4, 1], trials=3, kind='sweep')
            first = runner.checkpoint['trials'][0]
            request = lib.json_format.ParseDict(first['request'], lib.Control())
            client.call(request)
            response = client.get(request.RequestId)
            response.Run.WriteIOPS = 90
            runner.record_result(0, response)
            resumed = lib.Runner.resume(client, runner.directory, 'sweep')
            emitted = []
            self.assertTrue(resumed.execute(emitted.append))
            self.assertEqual(server.calls.count(lib.Control.START), 6)
            self.assertEqual(len(list(runner.directory.glob('result-*.json'))), 6)
            summary = json.loads((runner.directory / 'summary.json').read_text())
            self.assertEqual([row['inflight'] for row in summary], [4, 1])
            self.assertEqual(summary[0]['median_trial'], 2)
            calls_before = len(server.calls)
            self.assertTrue(lib.Runner.resume(client, runner.directory, 'sweep').execute(lambda _: None))
            self.assertEqual(len(server.calls), calls_before)

    def test_unconfirmed_cancellation_blocks_following_trial(self):
        server = Server()
        server.active = True
        client = self.client(server)
        transport = client.transport

        def never_stops(request):
            response = transport(request)
            server.active = True
            if response.HasField('Run'):
                response.Run.State = lib.Result.STOPPING
                response.Run.TerminationConfirmed = False
            return response

        client.transport = never_stops
        with tempfile.TemporaryDirectory() as root:
            runner = lib.Runner.create(client, Path(root) / 'run', config(), inflights=[1, 2], kind='sweep', wait_timeout=3)
            with self.assertRaisesRegex(lib.LoadError, 'unresolved'):
                runner.execute(lambda _: None)
            self.assertEqual(server.calls.count(lib.Control.START), 1)
            self.assertEqual(runner.checkpoint['trials'][0]['state'], 'accepted')
            self.assertEqual(runner.checkpoint['trials'][1]['state'], 'prepared')

    def test_retry_through_second_gateway_keeps_request_identity(self):
        import grpc
        from types import SimpleNamespace
        from ydb.core.protos import grpc_pb2_grpc, msgbus_pb2

        class LostReply(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE

        endpoints = {name: SimpleNamespace(host_with_grpc_port=name + ':2135', protocol='grpc')
                     for name in ('first', 'second')}
        params = SimpleNamespace(grpc_endpoints=endpoints, token='secret')
        result = msgbus_pb2.TResponse(Status=1)
        result.NbsDbgLikeLoadControl.Status = 1
        with patch.object(grpc, 'insecure_channel'), patch.object(grpc_pb2_grpc, 'TGRpcServerStub') as stub:
            rpc = stub.return_value.TestShardControl
            rpc.side_effect = [LostReply(), result]
            command = lib.Control(Operation=lib.Control.START, RequestId='one',
                                  CoordinatorNodeId=2, Incarnation='inc', Database='/Root')
            lib.GrpcTransport(params)(command)
            self.assertEqual(rpc.call_count, 2)
            self.assertEqual(rpc.call_args_list[0].args[0].NbsDbgLikeLoadControl.RequestId,
                             rpc.call_args_list[1].args[0].NbsDbgLikeLoadControl.RequestId)
            self.assertLessEqual(rpc.call_args_list[1].kwargs['timeout'],
                                 rpc.call_args_list[0].kwargs['timeout'])

    def test_startup_timeout_is_bounded(self):
        from ydb.apps.dstool.lib import dstool_cmd_cluster_workload_nbs_dbg_like as command
        import argparse
        parser = argparse.ArgumentParser()
        command.add_options(parser)

        def run(startup):
            args = parser.parse_args(['run', '--database', '/Root', '--tablet-id', '42',
                                      '--duration-seconds', '30', '--inflight', '8', '--format', 'json',
                                      '--startup-timeout', str(startup)])
            args.dry_run = True
            out = io.StringIO()
            with contextlib.redirect_stdout(out), patch.object(
                    lib.GrpcTransport, '__call__', side_effect=AssertionError('network')):
                command.do(args)
            return json.loads(out.getvalue())

        self.assertEqual(run(lib.MAX_STARTUP_TIMEOUT)['request']['StartupTimeoutSeconds'],
                         lib.MAX_STARTUP_TIMEOUT)
        error = io.StringIO()
        with contextlib.redirect_stderr(error), self.assertRaises(SystemExit):
            run(lib.MAX_STARTUP_TIMEOUT + 1)
        self.assertIn('--startup-timeout', error.getvalue())

    def test_truncated_checkpoint_is_reported(self):
        client = self.client(Server())
        with tempfile.TemporaryDirectory() as root:
            runner = lib.Runner.create(client, Path(root) / 'run', config())
            complete = json.loads((runner.directory / 'checkpoint.json').read_text())
            broken = [{key: value for key, value in complete.items() if key != 'startup'},
                      {**complete, 'trials': [{'state': 'prepared'}]},
                      {**complete, 'trials': []}]
            for checkpoint in broken:
                lib.atomic_json(runner.directory / 'checkpoint.json', checkpoint)
                with self.assertRaisesRegex(lib.LoadError, 'missing required fields'):
                    lib.Runner.resume(client, runner.directory, 'run')
                with self.assertRaisesRegex(lib.LoadError, 'missing required fields'):
                    lib.Runner.from_handle(client, runner.directory)

    def test_registered_command_exposes_actions(self):
        from ydb.apps.dstool.lib import commands, dstool_cmd_cluster_workload_nbs_dbg_like as command
        from ydb.apps.dstool.lib.arg_parser import ArgumentParser
        actions = {
            'create': ['--owner-index', '1'],
            'list': [],
            'describe': ['--owner-index', '1'],
            'delete': ['--owner-index', '1'],
            'run': ['--tablet-id', '42', '--duration-seconds', '30', '--inflight', '8'],
            'results': ['--request-id', 'request'],
            'stop': ['--request-id', 'request'],
        }
        for action, options in actions.items():
            with self.subTest(action=action), patch.object(command, 'do') as handler:
                parser = ArgumentParser()
                subparsers = parser.add_subparsers(dest='global_command', required=True)
                mapping = commands.make_command_map_by_structure(subparsers)
                self.assertIn('cluster-workload-nbs-dbg-like', mapping)
                self.assertNotIn('nbs-dbg-like-load', mapping)
                args = parser.parse_args(['cluster', 'workload', 'nbs-dbg-like', action,
                                          '--database', '/Root', '--format', 'json', *options])
                self.assertEqual(args.nbs_dbg_like_load_action, action)
                commands.run_command(mapping, args)
                handler.assert_called_once_with(args)

    def test_no_top_level_nbs_dbg_like_command(self):
        from ydb.apps.dstool.lib import commands
        from ydb.apps.dstool.lib.arg_parser import ArgumentParser
        for name in ('nbs-dbg-like-load', 'cluster-workload-nbs-dbg-like'):
            with self.subTest(name=name):
                parser = ArgumentParser()
                commands.make_command_map_by_structure(parser.add_subparsers(dest='global_command', required=True))
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as error:
                        parser.parse_args([name, 'list'])
                self.assertNotEqual(error.exception.code, 0)

    def test_existing_cluster_workload_run_dispatch(self):
        from ydb.apps.dstool.lib import commands
        from ydb.apps.dstool.lib.arg_parser import ArgumentParser
        with patch.object(commands.cluster_workload_run, 'do') as handler:
            parser = ArgumentParser()
            mapping = commands.make_command_map_by_structure(parser.add_subparsers(dest='global_command', required=True))
            args = parser.parse_args(['cluster', 'workload', 'run', '--config-file', 'workload.yaml'])
            self.assertEqual(args.config_file, 'workload.yaml')
            commands.run_command(mapping, args)
            handler.assert_called_once_with(args)
