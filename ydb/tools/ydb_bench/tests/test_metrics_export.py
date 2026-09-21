import gzip
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ydb.tools.ydb_bench.lib import metrics_export as export
from ydb.tools.ydb_bench.lib.common import BenchmarkError


class MetricsExportTest(unittest.TestCase):
    def record(self, attempt=1):
        return dict(
            timestamp_unix=100.125,
            host='host',
            port=8765,
            role='dynamic',
            index=1,
            context=dict(attempt=attempt, repetition=1),
            counters={
                'sensors': [
                    dict(labels={'sensor': 'CpuMicrosec', 'execpool': 'User'}, value=0),
                    dict(labels={'sensor': 'Latency'}, hist={'bounds': [1, 10], 'buckets': [2, 3], 'inf': 4}),
                ]
            },
        )

    def test_mapping_preserves_zero_and_histogram(self):
        samples = list(export.snapshot_samples(self.record(), {'bench_run': 'run'}))
        self.assertEqual(5, len(samples))
        self.assertEqual((100125, 0), samples[0][1:])
        self.assertEqual('CpuMicrosec', samples[0][0]['__name__'])
        self.assertEqual([2, 5, 9, 9], [s[2] for s in samples[1:]])
        self.assertEqual('+Inf', samples[-2][0]['le'])
        self.assertEqual([], list(export.snapshot_samples({'error': 'offline'}, {})))

    def test_invalid_labels_rejected(self):
        record = self.record()
        record['counters']['sensors'][0]['labels']['bad label'] = 'x'
        with self.assertRaises(BenchmarkError):
            list(export.snapshot_samples(record, {}))

    def test_ydb_namespace_and_verification(self):
        record = self.record()
        record['context']['verification'] = True
        record['counters']['sensors'][0]['labels']['counters'] = 'utils'
        labels, _, _ = next(export.snapshot_samples(record, {}))
        self.assertEqual('utils_CpuMicrosec', labels['__name__'])
        self.assertEqual('verification', labels['bench_attempt'])
        self.assertNotIn('counters', labels)

    def test_invalid_histogram_rejected(self):
        record = self.record()
        record['counters']['sensors'][1]['hist']['bounds'] = []
        with self.assertRaises(BenchmarkError):
            list(export.snapshot_samples(record, {}))

    def test_no_archive_fails_without_network(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'run.json').write_text('{"runs": []}')
            settings = mock.Mock()
            settings.snapshot.return_value = {'settings': dict(prometheus_url='http://prometheus', prometheus_token='')}
            exporter = export.MetricsExporter(root, settings, 'owner')
            with mock.patch.object(export, 'build_opener') as opener:
                exporter.start('run', root, {})
                exporter.thread.join(10)
                self.assertEqual('failed', exporter.status('run', {})['state'])
                opener.assert_not_called()

    def test_wire_golden(self):
        # Raw Snappy literal, WriteRequest { timeseries { labels {name:__name__,value:x} samples {value:1,timestamp:1}}}.
        expected = bytes.fromhex('1e740a1c0a0d0a085f5f6e616d655f5f120178120b09000000000000f03f1001')
        actual = export.encode_write([({'__name__': 'x'}, 1, 1.0)])
        self.assertEqual(expected, actual)

    def test_background_export_selection_and_idempotence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = root / 'run'
            archive = run / 'local-ydb' / 'p' / 'ydb-counters'
            archive.mkdir(parents=True)
            (run / 'run.json').write_text(
                json.dumps({'runs': [dict(benchmark='local-ydb', profile='p', directory='local-ydb/p')]})
            )
            with gzip.open(archive / 'part.jsonl.gz', 'wt') as stream:
                for attempt in (1, 2):
                    stream.write(json.dumps(self.record(attempt)) + '\n')
            settings = mock.Mock()
            settings.snapshot.return_value = {
                'settings': dict(prometheus_url='http://prometheus', prometheus_token='secret')
            }
            exporter = export.MetricsExporter(root, settings, 'owner')
            selection = dict(benchmark='local-ydb', profile='p', attempt='1')
            response = mock.MagicMock()
            response.__enter__.return_value.status = 204
            query_response = mock.MagicMock()
            query_response.__enter__.return_value.status = 200
            query_response.__enter__.return_value.read.return_value = json.dumps(
                dict(status='success', data=dict(resultType='matrix', result=[]))
            ).encode()
            with mock.patch.object(export, 'build_opener') as opener:
                opener.return_value.open.return_value = response
                opener.return_value.open.side_effect = lambda request, **kw: (
                    query_response if request.full_url.endswith('/query') else response
                )
                exporter.start('run', run, selection)
                exporter.thread.join(10)
                status = exporter.status('run', selection)
                self.assertEqual('completed', status['state'], status)
                self.assertEqual(5, status['samples'])
                exporter.start('run', run, selection)
                self.assertEqual(2, opener.return_value.open.call_count)
                request = opener.return_value.open.call_args.args[0]
                self.assertEqual('http://prometheus/api/v1/write', request.full_url)
                self.assertEqual('Bearer secret', request.get_header('Authorization'))
                # Another destination is a separate export; an HTTP failure is not success.
                settings.snapshot.return_value['settings']['prometheus_url'] = 'http://other'
                opener.return_value.open.side_effect = export.HTTPError('http://other', 401, 'secret', {}, None)
                exporter.start('run', run, selection)
                exporter.thread.join(10)
                failed = exporter.status('run', selection)
                self.assertEqual('failed', failed['state'])
                self.assertEqual(0, failed['samples'])
                self.assertNotIn('secret', failed['error'])
                opener.return_value.open.side_effect = lambda request, **kw: (
                    query_response if request.full_url.endswith('/query') else response
                )
                exporter.start('run', run, selection)
                exporter.thread.join(10)
                self.assertEqual('completed', exporter.status('run', selection)['state'])

    def test_precheck_skips_only_exact_samples_and_rejects_conflicts(self):
        with tempfile.TemporaryDirectory() as root, sqlite3.connect(':memory:') as db:
            settings = mock.Mock()
            settings.snapshot.return_value = {'settings': dict(prometheus_url='http://prometheus', prometheus_token='')}
            exporter = export.MetricsExporter(root, settings, 'owner')
            db.execute('CREATE TABLE series (id INTEGER PRIMARY KEY, labels TEXT UNIQUE)')
            db.execute(
                'CREATE TABLE samples (series INTEGER,timestamp INTEGER,value REAL,PRIMARY KEY(series,timestamp))'
            )
            labels = {'__name__': 'test', 'bench_run': 'run'}
            db.execute(
                'INSERT INTO series VALUES (?,?)', (1, json.dumps(labels, sort_keys=True, separators=(',', ':')))
            )
            db.executemany('INSERT INTO samples VALUES (1,?,?)', [(100125, 0), (100126, 1)])
            response = mock.MagicMock()
            response.__enter__.return_value.status = 200

            def reply(sample):
                response.__enter__.return_value.read.return_value = json.dumps(
                    dict(
                        status='success', data=dict(resultType='matrix', result=[dict(metric=labels, values=[sample])])
                    )
                ).encode()

            with mock.patch.object(export, 'build_opener') as opener:
                opener.return_value.open.return_value = response
                reply([100.125, '0'])
                self.assertEqual(1, exporter._skip_existing(db, 'run', {}, exporter._settings()))
                self.assertEqual([(1, 100126, 1)], db.execute('SELECT * FROM samples').fetchall())
                reply([100.126, '2'])
                with self.assertRaisesRegex(BenchmarkError, 'different value'):
                    exporter._skip_existing(db, 'run', {}, exporter._settings())
                response.__enter__.return_value.read.return_value = b'{"status":"error"}'
                with self.assertRaises(BenchmarkError):
                    exporter._skip_existing(db, 'run', {}, exporter._settings())

    def test_settings_generation_invalidates_status(self):
        with tempfile.TemporaryDirectory() as root:
            settings = mock.Mock()
            settings.snapshot.return_value = {
                'prometheus_revision': 1,
                'settings': dict(prometheus_url='http://p', prometheus_token=''),
            }
            exporter = export.MetricsExporter(root, settings, 'owner')
            key = exporter.status('r', {})['key']
            exporter.root.mkdir()
            (exporter.root / (key + '.json')).write_text('{"state":"completed","samples":5}')
            settings.snapshot.return_value['prometheus_revision'] = 2
            self.assertEqual('ready', exporter.status('r', {})['state'])

    def test_unconfigured(self):
        with tempfile.TemporaryDirectory() as root:
            settings = mock.Mock()
            settings.snapshot.return_value = {'settings': {'prometheus_url': ''}}
            exporter = export.MetricsExporter(root, settings, 'owner')
            with self.assertRaises(BenchmarkError):
                exporter.start('run', Path(root), {})

    def test_run_service_accepts_passed_but_rejects_running(self):
        from ydb.tools.ydb_bench.lib import web

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'run').mkdir()
            service = mock.Mock(output=root)
            service.detail.return_value = {'state': 'passed', 'status': 'completed'}
            web.RunService.metrics_export(service, 'run', {}, start=True)
            service.metrics_exporter.start.assert_called_once()
            service.detail.return_value = {'state': 'running', 'status': 'running'}
            with self.assertRaises(BenchmarkError):
                web.RunService.metrics_export(service, 'run', {}, start=True)
