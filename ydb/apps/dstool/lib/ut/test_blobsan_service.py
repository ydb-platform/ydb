import gc
import io
from itertools import permutations
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import warnings

from ydb.apps.dstool import main as cli
from ydb.apps.dstool.lib import blobsan_service as svc
from ydb.apps.dstool.lib import commands, common, write2x2_report
from ydb.apps.dstool.lib import dstool_cmd_blobsan_service as service_command
from ydb.apps.dstool.lib.arg_parser import ArgumentParser
from ydb.core.protos import blobstorage_config_pb2 as bsconfig


BLOB = '[42:7:258:3:5:4096:0]'


def layout(gid=1, nodes=None):
    nodes = nodes or list(range(1, 10))
    return dict(id=gid, generation=1, pool='static', fingerprint='v1',
                slots=[dict(index=i, node=node, pdisk=1, slot=100 + i) for i, node in enumerate(nodes)])


def args(**kwargs):
    values = dict(interval=3600, retry=60, refresh=300, lease=120, viewer='http://localhost:8765',
                  endpoint=['localhost'], request_timeout=5, scan_timeout=30, repair_delay=0,
                  master='unused', secret_file=None, poll=1, heartbeat=1, scan_only=False)
    values.update(kwargs)
    return SimpleNamespace(**values)


def scan_report(completed=tuple(range(9)), gid=1, blobs=1):
    local = '+'.join('1' if all(r * 3 + d in completed for d in range(3)) else '?' for r in range(3))
    buckets = [dict(local=local, ingress='1+1+1', gc='no_covering_barrier', blobs=blobs,
                    logical_bytes=100 * blobs, observed_replica_bytes=300 * blobs, missing_metadata_blobs=0)] if blobs else []
    return dict(version=1, group_id=gid, completed_vdisks=sorted(completed), buckets=buckets,
                totals=write2x2_report.totals(buckets))


class TestMaster(unittest.TestCase):
    def setUp(self):
        svc.STOP.clear()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.master = svc.Master(args(), self.directory)
        self.addCleanup(lambda: self.master.db.close())
        self.add_group(layout())

    def add_group(self, item):
        with self.master.db:
            self.master.db.execute('INSERT INTO groups(id,layout,seen) VALUES(?,?,?)',
                                   (item['id'], json.dumps(item), time.time()))

    def claim(self, worker='one'):
        return self.master.rpc('/claim', {'worker': worker})['job']

    def result(self, job, findings, worker='one', completed=None):
        completed = list(range(9)) if completed is None else completed
        result = dict(findings=findings, completed_vdisks=completed,
                      missing_vdisks=sorted(set(range(9)) - set(completed)))
        return self.master.rpc('/result', dict(worker=worker, group=job['layout']['id'], token=job['token'], result=result))

    def observation(self, job, completed=tuple(range(9)), blobs=1):
        report = scan_report(completed, job['layout']['id'], blobs)
        now = time.time()
        result = dict(findings={}, findings_complete=False, scan_report=report,
                      completed_vdisks=list(completed), missing_vdisks=sorted(set(range(9)) - set(completed)),
                      started_at=now - 2, finished_at=now - 1)
        body = dict(worker='one', group=job['layout']['id'], token=job['token'], result=result)
        self.assertTrue(self.master.rpc('/result', body)['accepted'])
        return body

    def test_pool_uses_latest_group_reports_and_survives_restart(self):
        self.add_group(layout(2, list(range(11, 20))))
        job = self.claim()
        body = self.observation(job, blobs=2)
        self.master.rpc('/result', body)
        self.observation(self.claim(), blobs=3)
        pool, = json.loads(self.master.console('pools static'))
        self.assertEqual(pool['totals']['blobs'], 5)
        self.assertEqual(pool['full_groups'], 2)
        self.master.console('scan 1')
        self.observation(self.claim(), blobs=1)
        self.master.db.close()
        self.master = svc.Master(args(), self.directory)
        pool, = json.loads(self.master.console('pools'))
        self.assertEqual(pool['totals']['blobs'], 4)
        self.assertEqual(pool['totals']['logical_bytes'], 400)

    def test_groups_brief_omits_reports_and_includes_pool_identity(self):
        self.observation(self.claim())
        group = json.loads(self.master.console('groups brief'))
        self.assertEqual(group['id'], 1)
        self.assertEqual(group['pool'], 'static')
        self.assertEqual(group['pool_key'], 'static')
        self.assertEqual(group['generation'], 1)
        self.assertNotIn('last_result', group)
        self.assertNotIn('layout', group)
        self.assertIn('last_result', json.loads(self.master.console('groups')))

    def test_observation_accepts_partial_coverage_and_preserves_findings(self):
        self.result(self.claim(), {BLOB: 'request_ok'})
        self.master.console('scan 1')
        self.observation(self.claim(), completed=(0, 1))
        pool, = json.loads(self.master.console('pools'))
        self.assertEqual(pool['partial_groups'], 1)
        self.assertEqual(pool['buckets'][0]['local'], '?+?+?')
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 1)
        self.assertIsNone(json.loads(self.master.console('layouts 1'))['candidates'])
        self.master.console('scan 1')
        self.observation(self.claim())
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 1)

    def test_failed_attempt_preserves_previous_scan_report(self):
        self.observation(self.claim())
        self.master.console('scan 1')
        job = self.claim()
        self.master.rpc('/result', dict(worker='one', group=1, token=job['token'], result={'error': 'all disks unavailable'}))
        detail = json.loads(self.master.console('layouts 1'))
        self.assertEqual(detail['latest_error'], 'all disks unavailable')
        self.assertEqual(detail['scan_report']['totals']['blobs'], 1)
        pool, = json.loads(self.master.console('pools'))
        self.assertEqual(pool['failed_groups'], 1)
        self.assertEqual(pool['totals']['blobs'], 1)

    def test_outdated_and_stale_inventory_reports_are_excluded(self):
        self.observation(self.claim())
        item = layout()
        item.update(generation=2, fingerprint='v2')
        with self.master.db:
            self.master.db.execute('UPDATE groups SET layout=?', (json.dumps(item),))
        pool, = json.loads(self.master.console('pools'))
        self.assertEqual(pool['outdated_groups'], 1)
        self.assertEqual(pool['totals']['blobs'], 0)
        self.assertTrue(json.loads(self.master.console('layouts 1'))['outdated'])
        with self.master.db:
            self.master.db.execute('UPDATE groups SET seen=0')
        pool, = json.loads(self.master.console('pools'))
        self.assertEqual(pool['stale_inventory_groups'], 1)
        self.assertEqual(pool['reported_groups'], 0)

    def test_pool_identity_and_never_scanned_groups(self):
        for gid, key in [(1, '1:2'), (2, '2:2')]:
            item = dict(layout(gid, list(range(gid * 10, gid * 10 + 9))), pool='same-name', pool_key=key)
            with self.master.db:
                self.master.db.execute('INSERT OR REPLACE INTO groups(id,layout,seen) VALUES(?,?,?)',
                                       (gid, json.dumps(item), time.time()))
        self.observation(self.claim())
        pools = json.loads(self.master.console('pools same-name'))
        self.assertEqual(len(pools), 2)
        self.assertEqual(pools[1]['never_scanned_groups'], 1)
        self.assertEqual(json.loads(self.master.console('pools 1:2'))[0]['totals']['blobs'], 1)

    def test_legacy_inventory_without_pool_ids_does_not_merge_dynamic_pool_names(self):
        for gid in (0x80000001, 0x80000002):
            self.add_group(dict(layout(gid), pool='same-name'))
        pools = json.loads(self.master.console('pools same-name'))
        self.assertEqual([pool['pool_key'] for pool in pools], ['unknown:2147483649', 'unknown:2147483650'])
        self.assertTrue(all(pool['groups'] == 1 for pool in pools))

    def test_retry_claim_node_exclusion_and_pause(self):
        self.add_group(layout(2))
        self.add_group(layout(3, list(range(11, 20))))
        job = self.claim()
        self.assertEqual(self.claim()['token'], job['token'])
        self.assertEqual(self.claim('two')['layout']['id'], 3)
        self.assertIsNone(self.claim('three'))
        self.master.console('pause')
        self.assertEqual(json.loads(self.master.console('stats'))['paused'], '1')

    def test_discovery_assigns_dynamic_group_from_protobuf(self):
        gid = 0x80000001
        base = bsconfig.TBaseConfig()
        group = base.Group.add(GroupId=gid, GroupGeneration=7, ErasureSpecies='mirror-3-dc',
                               BoxId=1, StoragePoolId=2)
        for index in range(9):
            slot_id = group.VSlotId.add(NodeId=11 + index, PDiskId=1, VSlotId=100 + index)
            slot = base.VSlot.add(GroupId=gid, GroupGeneration=7, FailRealmIdx=index // 3,
                                  FailDomainIdx=index % 3, VDiskIdx=0)
            slot.VSlotId.CopyFrom(slot_id)
        base.Group.add(GroupId=gid + 1, ErasureSpecies='block-4-2')
        virtual = base.Group.add(GroupId=gid + 2, ErasureSpecies='mirror-3-dc')
        virtual.VirtualGroupInfo.SetInParent()
        pool = bsconfig.TDefineStoragePool(BoxId=1, StoragePoolId=2, Name='dynamic-pool')
        data = dict(BaseConfig=base, StoragePools=[pool])
        with patch.object(common, 'fetch_base_config_and_storage_pools', return_value=data), \
                patch.object(svc, 'view', return_value={}), \
                patch.object(self.master.refresh, 'wait', side_effect=lambda *_: svc.STOP.set()):
            try:
                self.master.discover()
            finally:
                svc.STOP.clear()
        self.assertEqual(json.loads(self.master.console('stats'))['discovery_error'], '')
        self.assertEqual(json.loads(self.master.console('stats'))['groups'], 2)
        self.claim('static-worker')
        discovered = self.claim('dynamic-worker')['layout']
        self.assertEqual(discovered['id'], gid)
        self.assertEqual(discovered['generation'], 7)
        self.assertEqual(discovered['pool'], 'dynamic-pool')
        self.assertEqual([s['index'] for s in discovered['slots']], list(range(9)))
        self.assertEqual([s['node'] for s in discovered['slots']], list(range(11, 20)))

    def test_expired_lease_rejects_old_results(self):
        old = self.claim()
        with self.master.db:
            self.master.db.execute('UPDATE groups SET expires=0')
        new = self.claim('two')
        self.assertFalse(self.result(old, {BLOB: 'request_ok'})['accepted'])
        self.assertTrue(self.result(new, {}, 'two')['accepted'])

    def test_result_idempotence_reappearance_and_restart(self):
        job = self.claim()
        self.assertTrue(self.result(job, {BLOB: 'request_ok'})['accepted'])
        self.assertTrue(self.result(job, {BLOB: 'request_ok'})['accepted'])
        self.assertEqual(json.loads(self.master.console('stats'))['scans'], 1)
        self.master.db.close()
        self.master = svc.Master(args(), self.directory)
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 1)
        self.master.console('scan all')
        self.result(self.claim(), {})
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 0)
        self.master.console('scan 1')
        self.result(self.claim(), {BLOB: 'absent'})
        self.assertEqual(json.loads(self.master.console('stats'))['new_total'], 2)

    def test_failed_scan_preserves_findings(self):
        self.result(self.claim(), {BLOB: 'request_ok'})
        self.master.console('scan all')
        job = self.claim()
        self.master.rpc('/result', dict(worker='one', group=1, token=job['token'], result={'error': 'stream lost'}))
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 1)

    def test_coverage_change_does_not_resolve_findings(self):
        self.result(self.claim(), {BLOB: 'request_ok'}, completed=list(range(6)))
        self.master.console('scan all')
        self.result(self.claim(), {}, completed=list(range(3, 9)))
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 1)
        self.master.console('scan all')
        self.result(self.claim(), {})
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 0)

    def test_repeated_changed_coverage_preserves_findings_after_restart(self):
        self.result(self.claim(), {BLOB: 'request_ok'}, completed=list(range(6)))
        self.master.console('scan all')
        self.result(self.claim(), {}, completed=list(range(3, 9)))
        self.master.db.close()
        self.master = svc.Master(args(), self.directory)
        self.master.console('scan all')
        self.result(self.claim(), {}, completed=list(range(3, 9)))
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 1)
        row = json.loads(self.master.console('groups'))
        self.assertAlmostEqual(row['due'] - time.time(), args().retry, delta=5)
        self.assertEqual(json.loads(row['last_result'])['disappeared'], 0)
        self.assertEqual(json.loads(row['last_result'])['unresolved'], 1)
        self.master.console('scan all')
        self.result(self.claim(), {}, completed=list(range(6)))
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 0)

    def test_findings_resolved_only_against_their_own_coverage(self):
        other_blob = '[42:7:259:3:5:4096:0]'
        self.result(self.claim(), {BLOB: 'request_ok'}, completed=list(range(6)))
        self.master.console('scan all')
        self.result(self.claim(), {other_blob: 'absent'}, completed=list(range(3, 9)))
        self.master.console('scan all')
        self.result(self.claim(), {}, completed=list(range(3, 9)))
        active = [json.loads(line)['blob'] for line in self.master.console('findings 1').splitlines()]
        self.assertEqual(active, [BLOB])
        result = json.loads(json.loads(self.master.console('groups'))['last_result'])
        self.assertEqual(result['disappeared'], 1)

    def test_invalid_coverage_rolls_back(self):
        job = self.claim()
        with self.assertRaises(ValueError):
            self.result(job, {}, completed=[0, 1, 2])
        self.assertEqual(json.loads(self.master.console('stats'))['scans'], 0)

    def test_http_transport_and_auth(self):
        server = svc.http_server.ThreadingHTTPServer(('127.0.0.1', 0), svc.API)
        server.master, server.api_secret = self.master, 'secret'
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            url = 'http://127.0.0.1:%d/claim' % server.server_port
            with self.assertRaises(svc.urllib.error.HTTPError) as failure:
                svc.http(url, data={'worker': 'one'})
            self.assertTrue(failure.exception.fp.closed)
            result = svc.http(url, {'Authorization': 'Bearer secret'}, {'worker': 'one'})
            self.assertEqual(result['job']['layout']['id'], 1)
        finally:
            server.shutdown()
            thread.join()
            server.server_close()

    def test_console_socket(self):
        path = str(self.directory / 'console.sock')
        server = svc.UnixServer(path, svc.Console)
        server.master = self.master
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            with socket.socket(socket.AF_UNIX) as conn:
                conn.settimeout(3)
                conn.connect(path)
                self.assertIn(b'groups', conn.recv(8192))
                conn.sendall(b'pause\n')
                self.assertIn(b'OK', conn.recv(8192))
                conn.sendall(b'quit\n')
            self.assertEqual(self.master.setting('paused'), 1)
        finally:
            server.shutdown()
            thread.join()
            server.server_close()

    def test_worker_repairs_then_verifies_over_http(self):
        repaired, reports = False, []
        sequences = {}
        generation_checks = []

        def ydb(_args, url, raw=False):
            nonlocal repaired
            parsed = svc.urllib.parse.urlsplit(url)
            if parsed.path.endswith('bsgroupinfo'):
                self.assertEqual(svc.urllib.parse.parse_qs(parsed.query),
                                 {'node_id': ['1,2,3,4,5,6,7,8,9'], 'filter': ['GroupID=1']})
                generation_checks.append((len(sequences), repaired))
                return {'BSGroupStateInfo': [{'GroupID': 1, 'GroupGeneration': 1, 'ErasureSpecies': 'mirror-3-dc'}]}
            if parsed.path.endswith('tenantinfo'):
                return {'TenantInfo': []}
            if parsed.path.endswith('sysinfo'):
                return {'SystemStateInfo': [{'NodeId': 1}]}
            if parsed.path.endswith('blob_range'):
                repaired = True
                return {'Status': 'OK', 'Blobs': [BLOB]}
            params = svc.urllib.parse.parse_qs(parsed.query)
            session = params['sessionId'][0]
            if session in sequences:
                return b''
            sequences[session] = True
            node = int(parsed.path.split('/')[2])
            return b'opaque snapshot bytes for node %d' % node

        original = self.master.rpc

        def rpc(action, body):
            result = original(action, body)
            if action == '/result':
                reports.append(body['result'])
                if len(reports) == 1:
                    self.master.console('scan all')
                else:
                    svc.STOP.set()
            return result

        server = svc.http_server.ThreadingHTTPServer(('127.0.0.1', 0), svc.API)
        server.master, server.api_secret = self.master, ''
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.start()
        directory = self.directory / 'worker'
        directory.mkdir()
        timer = threading.Timer(10, svc.STOP.set)
        timer.start()
        try:
            with patch.object(svc, 'ydb_http', side_effect=ydb), patch.object(self.master, 'rpc', side_effect=rpc), \
                    patch.object(svc, 'run_blobsan', side_effect=lambda *_: (
                        set() if repaired else {BLOB}, scan_report(blobs=0 if repaired else 1))):
                svc.work(args(master='127.0.0.1:%d' % server.server_port), directory)
            self.assertEqual(len(reports), 2)
            self.assertEqual(reports[0]['findings'], {BLOB: 'request_ok'})
            self.assertEqual(reports[1]['findings'], {})
            self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 0)
            self.assertEqual(generation_checks, [
                (0, False), (9, False), (9, True), (9, True), (18, True), (18, True),
            ])
        finally:
            timer.cancel()
            server.shutdown()
            server_thread.join()
            server.server_close()
            svc.STOP.clear()

    def test_scan_only_worker_publishes_report_without_repair_or_resolving_findings(self):
        self.result(self.claim(), {BLOB: 'request_ok'})
        self.master.console('scan 1')
        directory = self.directory / 'worker'
        directory.mkdir()
        now = time.time()
        coverage = dict(completed_vdisks=list(range(9)), missing_vdisks=[], stream_errors={},
                        scan_report=scan_report(), started_at=now - 2, finished_at=now - 1)

        def rpc(url, headers, data, **kwargs):
            action = '/' + url.rsplit('/', 1)[1]
            reply = self.master.rpc(action, data)
            if action == '/result':
                svc.STOP.set()
            return reply

        with patch.object(svc, 'http', side_effect=rpc), patch.object(svc, 'validate_layout'), \
                patch.object(svc, 'scan', return_value=({BLOB}, coverage)), patch.object(svc, 'repair') as repair:
            svc.work(args(scan_only=True), directory)
        repair.assert_not_called()
        self.assertEqual(json.loads(self.master.console('layouts 1'))['scan_report']['totals']['blobs'], 1)
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 1)
        svc.STOP.clear()


class TestWorker(unittest.TestCase):
    def setUp(self):
        svc.STOP.clear()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)

    def test_generation_check_is_scoped_and_fresh(self):
        for gid in (1, 0x80000001):
            job = {'layout': layout(gid, [3, 1, 2, 3, 1, 2, 3, 1, 2])}
            current = dict(GroupID=gid, GroupGeneration=1, ErasureSpecies='mirror-3-dc')
            newer = dict(current, GroupGeneration=2)
            with self.subTest(gid=gid), patch.object(svc, 'ydb_http', side_effect=[
                {'BSGroupStateInfo': [current]}, {'BSGroupStateInfo': [current, newer]},
            ]) as fetch:
                svc.validate_layout(args(), job)
                with self.assertRaisesRegex(ValueError, 'generation changed'):
                    svc.validate_layout(args(), job)
                self.assertEqual(fetch.call_count, 2)
                for call in fetch.call_args_list:
                    parsed = svc.urllib.parse.urlsplit(call.args[1])
                    self.assertEqual(parsed.path, '/viewer/json/bsgroupinfo')
                    self.assertEqual(svc.urllib.parse.parse_qs(parsed.query),
                                     {'node_id': ['1,2,3'], 'filter': ['GroupID=%d' % gid]})

    def test_generation_check_rejects_missing_or_incompatible_group(self):
        current = dict(GroupID=1, GroupGeneration=1, ErasureSpecies='mirror-3-dc')
        for response in ({}, {'Error': 'unavailable'}, {'BSGroupStateInfo': [dict(current, GroupID=2)]},
                         {'BSGroupStateInfo': [dict(current, GroupGeneration=0)]},
                         {'BSGroupStateInfo': [dict(current, ErasureSpecies='block-4-2')]}):
            with self.subTest(response=response), patch.object(svc, 'ydb_http', return_value=response):
                with self.assertRaises(ValueError):
                    svc.validate_layout(args(), {'layout': layout()})
        item = layout()
        item['slots'] = []
        with patch.object(svc, 'ydb_http') as fetch:
            with self.assertRaisesRegex(ValueError, 'No group nodes'):
                svc.validate_layout(args(), {'layout': item})
            fetch.assert_not_called()

    def test_relative_blobsan_runs_checked_executable(self):
        binary = self.directory / 'blobsan'
        binary.write_text('#!/bin/sh\n'
                          '[ "$#" -eq 6 ] && [ "$1" = --write2x2-candidates ] && [ "$2" = --scan-manifest ] '
                          '&& [ "$4" = --scan-report ] && [ "$6" = mirror-3-dc ] || exit 2\n'
                          'IFS= read -r snapshot\n'
                          '[ "$snapshot" = "opaque snapshot bytes" ] || exit 3\n'
                          "/bin/cat > \"$5\" <<'REPORT'\n" + json.dumps(scan_report()) + '\nREPORT\n'
                          "printf '%s\\n' 'GroupId# 1 BlobId# " + BLOB + "' >&2\n")
        binary.chmod(0o700)
        (self.directory / 'snapshot').write_bytes(b'opaque snapshot bytes\n')
        svc.atomic_json(self.directory / 'manifest.json', dict(group_id=1, completed_vdisks=list(range(9))))
        decoy_dir = self.directory / 'bin'
        decoy_dir.mkdir()
        decoy = decoy_dir / 'blobsan'
        decoy.write_text('#!/bin/sh\nexit 99\n')
        decoy.chmod(0o700)

        def work(parsed, directory):
            self.assertEqual(parsed.blobsan, str(binary.resolve()))
            for scan_only in (False, True):
                parsed.scan_only = scan_only
                self.assertEqual(svc.run_blobsan(parsed, 1, self.directory, threading.Event()), ({BLOB}, scan_report()))

        previous_cwd = Path.cwd()
        try:
            os.chdir(self.directory)
            with patch.dict(os.environ, {'PATH': str(decoy_dir)}), \
                    patch.object(common, 'connection_params', common.ConnectionParams()), \
                    patch.object(svc, 'work', side_effect=work) as worker:
                service_command.do(args(blobsan_service_action='worker', dry_run=False,
                                        blobsan='./blobsan', db=str(self.directory / 'db')))
                worker.assert_called_once()
        finally:
            os.chdir(previous_cwd)

    def test_scan_accepts_partial_coverage_and_rejects_no_complete_streams(self):
        def run(failed):
            counts = {}

            def fetch(_args, url, raw=False):
                parsed = svc.urllib.parse.urlsplit(url)
                node = int(parsed.path.split('/')[2])
                counts[node] = counts.get(node, 0) + 1
                if node in failed:
                    raise OSError('unavailable')
                if counts[node] == 1:
                    return b'opaque snapshot bytes'
                return b''

            def analyze(_args, gid, directory, cancel):
                manifest = json.loads((directory / 'manifest.json').read_text())
                return {BLOB}, scan_report(manifest['completed_vdisks'])

            with patch.object(svc, 'ydb_http', side_effect=fetch), patch.object(svc, 'run_blobsan', side_effect=analyze):
                return svc.scan(args(), {'layout': layout()}, self.directory, threading.Event())

        found, coverage = run({7, 8, 9})
        self.assertEqual(found, {BLOB})
        self.assertEqual(coverage['missing_vdisks'], [6, 7, 8])
        _, coverage = run({6, 7, 8, 9})
        self.assertEqual(coverage['completed_vdisks'], list(range(5)))
        with self.assertRaises(RuntimeError):
            run(set(range(1, 10)))

    def test_repair_json_semantics_and_expired_local_lease(self):
        responses = [{'TenantInfo': []}, {'SystemStateInfo': [{'NodeId': 10}]}, {'Status': 'OK', 'Blobs': []}]
        with patch.object(svc, 'ydb_http', side_effect=responses):
            self.assertEqual(svc.repair(args(), {'layout': layout()}, {BLOB}, threading.Event()), {BLOB: 'absent'})
        cancel = threading.Event()
        cancel.deadline = time.monotonic() - 1
        with self.assertRaises(RuntimeError):
            svc.check_running(cancel)

    def test_failed_stream_payload_is_discarded_and_empty_streams_are_in_manifest(self):
        calls = {}

        def fetch(_args, url, raw=False):
            node = int(svc.urllib.parse.urlsplit(url).path.split('/')[2])
            if node != 9:
                return b''
            calls[node] = calls.get(node, 0) + 1
            if calls[node] == 1:
                return b'partial payload'
            raise OSError('stream interrupted')

        completed = list(range(8))
        with patch.object(svc, 'ydb_http', side_effect=fetch), \
                patch.object(svc, 'run_blobsan', return_value=(set(), scan_report(completed, blobs=0))):
            _, coverage = svc.scan(args(), {'layout': layout()}, self.directory, threading.Event())
        self.assertEqual(coverage['completed_vdisks'], completed)
        self.assertEqual(coverage['stream_errors'], {8: 'stream interrupted'})
        self.assertEqual((self.directory / 'snapshot').read_bytes(), b'')
        self.assertEqual(json.loads((self.directory / 'manifest.json').read_text())['completed_vdisks'], completed)

    def test_pending_result_replayed_after_restart(self):
        svc.atomic_json(self.directory / 'worker.json', {'id': 'worker'})
        svc.atomic_json(self.directory / 'pending.json', {'group': 1, 'token': 'lease', 'result': {'error': 'test'}})

        def reply(url, headers, data, **kwargs):
            self.assertTrue(url.endswith('/result'))
            self.assertEqual(data['token'], 'lease')
            svc.STOP.set()
            return {'accepted': True}

        try:
            with patch.object(svc, 'http', side_effect=reply):
                svc.work(args(), self.directory)
            self.assertFalse((self.directory / 'pending.json').exists())
        finally:
            svc.STOP.clear()

    def test_missing_master_logs_one_line_and_keeps_retrying(self):
        self.addCleanup(svc.STOP.clear)
        # A bound socket without listen() reliably refuses connections while
        # keeping the port reserved for the duration of this test.
        with socket.socket() as unavailable:
            unavailable.bind(('127.0.0.1', 0))
            master = '127.0.0.1:%d' % unavailable.getsockname()[1]
            with self.assertLogs(svc.LOG, level='INFO') as logs, \
                    patch.object(svc.STOP, 'wait', side_effect=lambda _: svc.STOP.set()) as wait:
                svc.work(args(master=master, request_timeout=1, poll=5), self.directory)
        wait.assert_called_once_with(5)
        self.assertEqual(len(logs.records), 1)
        self.assertFalse(logs.records[0].exc_info)
        self.assertEqual(len(logs.output[0].splitlines()), 1)
        self.assertIn('Connection refused', logs.output[0])
        self.assertIn('retrying in 5s', logs.output[0])

    def test_failed_result_submission_preserves_pending_and_logs_one_line(self):
        self.addCleanup(svc.STOP.clear)
        pending = dict(group=1, token='lease', result={'error': 'test'})
        svc.atomic_json(self.directory / 'pending.json', pending)
        with self.assertLogs(svc.LOG, level='INFO') as logs, \
                patch.object(svc, 'http', side_effect=svc.urllib.error.URLError('connection\nrefused')), \
                patch.object(svc.STOP, 'wait', side_effect=lambda _: svc.STOP.set()):
            svc.work(args(), self.directory)
        self.assertEqual(json.loads((self.directory / 'pending.json').read_text()), pending)
        self.assertEqual(len(logs.output), 1)
        self.assertEqual(len(logs.output[0].splitlines()), 1)
        self.assertIn('connection refused', logs.output[0])
        self.assertFalse(logs.records[0].exc_info)

    def test_scratch_failure_reports_error_and_stops_heartbeat(self):
        (self.directory / 'scan').write_text('not a directory')
        replies = []

        def reply(url, headers, data, **kwargs):
            if url.endswith('/claim'):
                return {'job': dict(layout=layout(), token='lease', lease_seconds=120)}
            self.assertTrue(url.endswith('/result'))
            replies.append(data)
            svc.STOP.set()
            return {'accepted': True}

        with self.assertLogs(svc.LOG, level='ERROR') as logs, patch.object(svc, 'http', side_effect=reply):
            svc.work(args(), self.directory)
        self.assertEqual(len(replies), 1)
        self.assertIn('error', replies[0]['result'])
        self.assertEqual(len(logs.output), 1)
        self.assertFalse(logs.records[0].exc_info)
        svc.STOP.clear()


class TestHttp(unittest.TestCase):
    def test_responses_close_sockets_without_resource_warnings(self):
        class Handler(svc.http_server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == '/error':
                    self.send_error(503, 'Unavailable')
                    return
                body = b'' if self.path == '/empty' else b'{"ok":true}'
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_):
                pass

        server = svc.http_server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            gc.collect()
            with warnings.catch_warnings(record=True) as recorded:
                warnings.simplefilter('always', ResourceWarning)
                base = 'http://127.0.0.1:%d' % server.server_port
                for _ in range(10):
                    self.assertEqual(svc.http(base + '/ok'), {'ok': True})
                    self.assertEqual(svc.http(base + '/empty', raw=True), b'')
                    with self.assertRaises(svc.urllib.error.HTTPError) as failure:
                        svc.http(base + '/error')
                    self.assertEqual(failure.exception.code, 503)
                    self.assertTrue(failure.exception.fp.closed)
                    del failure
                gc.collect()
            self.assertEqual([str(w.message) for w in recorded if issubclass(w.category, ResourceWarning)], [])
        finally:
            server.shutdown()
            thread.join()
            server.server_close()


class TestCLI(unittest.TestCase):
    def setUp(self):
        version = patch.object(cli, 'get_version', return_value='test')
        version.start()
        self.addCleanup(version.stop)

    def test_service_endpoint_default_and_explicit_spellings(self):
        for action, tail in [('master', ['--console-sock', '/tmp/socket']),
                             ('worker', ['--master', 'host:22570'])]:
            command = ['blobsan-service', action, '--db', '/tmp/db'] + tail
            for endpoints, expected in [([], ['localhost']),
                                        (['-eremote.example'], ['remote.example']),
                                        (['-e', 'remote.example'], ['remote.example']),
                                        (['--endpoint', 'remote.example'], ['remote.example']),
                                        (['--endpoint=remote.example'], ['remote.example']),
                                        (['-efirst.example', '-esecond.example'], ['first.example', 'second.example'])]:
                argv = endpoints + command
                with self.subTest(argv=argv), \
                        patch.object(common, 'connection_params', common.ConnectionParams()), \
                        patch.object(common, 'apply_args'), patch.object(commands, 'run_command') as run:
                    cli.main(argv)
                    parsed = run.call_args.args[1]
                    self.assertEqual(parsed.endpoint, expected)
                    self.assertEqual(svc.viewer_url(parsed), 'http://' + expected[0] + ':8765')

    def test_other_commands_still_require_endpoint(self):
        with patch.object(common, 'apply_args') as apply_args, \
                patch.object(commands, 'run_command') as run, \
                patch('sys.stderr', new_callable=io.StringIO), patch('sys.stdout', new_callable=io.StringIO), \
                self.assertRaises(SystemExit) as error:
            cli.main(['cluster', 'list'])
        self.assertEqual(error.exception.code, 1)
        apply_args.assert_not_called()
        run.assert_not_called()

    def test_commands_parse(self):
        for tail in (['master', '-l', '22570', '--db', '/tmp/master', '--console-sock', '/tmp/socket'],
                     ['worker', '--master', 'host:22570', '--db', '/tmp/worker'],
                     ['client', '--console-sock', '/tmp/socket']):
            parser = ArgumentParser()
            command_map = commands.make_command_map_by_structure(
                parser.add_subparsers(dest='global_command', required=True))
            parsed = parser.parse_args(['blobsan-service'] + tail)
            self.assertEqual(parsed.global_command, 'blobsan-service')
            self.assertEqual(parsed.blobsan_service_action, tail[0])
            self.assertIs(command_map[parsed.global_command], service_command.do)

    def test_client_needs_no_db_or_endpoint(self):
        with patch.object(common, 'connection_params', common.ConnectionParams()), \
                patch.object(common, 'apply_args'), patch.object(commands, 'run_command') as run:
            cli.main(['blobsan-service', 'client', '--console-sock', '/tmp/socket'])
        parsed = run.call_args.args[1]
        self.assertEqual(parsed.blobsan_service_action, 'client')
        self.assertEqual(parsed.console_sock, '/tmp/socket')
        self.assertEqual(parsed.refresh, 5)
        self.assertFalse(hasattr(parsed, 'db'))

    def test_scan_only_flag(self):
        parser = ArgumentParser()
        commands.make_command_map_by_structure(parser.add_subparsers(dest='global_command', required=True))
        parsed = parser.parse_args(['blobsan-service', 'worker', '--master', 'host:22570',
                                    '--db', '/tmp/worker', '--scan-only'])
        self.assertTrue(parsed.scan_only)


class TestScanReportValidation(unittest.TestCase):
    def test_local3_and_local2_normalize_permutations_and_unknown_realms(self):
        cases = [('2+1+1', '2+1+1', '2+1'), ('1+2+?', '2+1+?', '2+1'),
                 ('0+?+0', '0+0+?', '0+0'), ('3+?+?', '3+?+?', '3+?'),
                 ('?+?+?', '?+?+?', '?+?'), ('1+3+2', '3+2+1', '3+2')]
        for source, local3, local2 in cases:
            for parts in set(permutations(source.split('+'))):
                local = '+'.join(parts)
                with self.subTest(local=local):
                    self.assertEqual(write2x2_report.local3(local), local3)
                    self.assertEqual(write2x2_report.local2(local), local2)

    def test_annotated_buckets_remain_distinct_from_legacy_and_other_annotations(self):
        report = scan_report(blobs=2)
        legacy = report['buckets'][0]
        candidate = dict(legacy, phantom_like=False, write2x2_candidate=True)
        phantom = dict(legacy, phantom_like=True, write2x2_candidate=False, missing_metadata_blobs=2)
        report['buckets'] = [legacy, candidate, phantom]
        report['totals'] = write2x2_report.totals(report['buckets'])
        write2x2_report.validate(report, 1, list(range(9)))
        histogram = {}
        write2x2_report.merge(histogram, report['buckets'])
        rows = write2x2_report.histogram_rows(histogram)
        self.assertEqual(len(rows), 3)
        self.assertIsNone(rows[0]['write2x2_candidate'])
        self.assertEqual(write2x2_report.totals(rows), report['totals'])

    def test_empty_and_partial_reports(self):
        for completed in (list(range(9)), [0, 1, 2], [7]):
            for blobs in (0, 2):
                report = scan_report(completed, blobs=blobs)
                self.assertEqual(write2x2_report.validate(report, 1, completed), report)

    def test_malformed_reports(self):
        mutations = [lambda r: r.update(version=2), lambda r: r.update(group_id=2),
                     lambda r: r.update(completed_vdisks=[0]), lambda r: r.update(totals={}),
                     lambda r: r['buckets'].append(r['buckets'][0]),
                     lambda r: r['buckets'][0].update(local='1+1+?'),
                     lambda r: r['buckets'][0].update(ingress='1+1+?'),
                     lambda r: r['buckets'][0].update(gc='unknown'),
                     lambda r: r['buckets'][0].update(blobs=-1),
                     lambda r: r['buckets'][0].update(blobs=True),
                     lambda r: r['buckets'][0].update(phantom_like=1),
                     lambda r: r['buckets'][0].update(write2x2_candidate='yes'),
                     lambda r: r['buckets'][0].update(phantom_like=True),
                     lambda r: r['buckets'][0].update(phantom_like=True, write2x2_candidate=True, missing_metadata_blobs=1),
                     lambda r: r['buckets'][0].update(phantom_like=False, write2x2_candidate=True, gc='hard_barrier'),
                     lambda r: r['buckets'][0].update(missing_metadata_blobs=2)]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                report = scan_report()
                mutate(report)
                with self.assertRaises(ValueError):
                    write2x2_report.validate(report, 1, list(range(9)))
