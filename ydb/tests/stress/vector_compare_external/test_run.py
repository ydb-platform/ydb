import copy
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('vector_compare', Path(__file__).with_name('run.py'))
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def manifests(dataset):
    dimension, metric, *_ = bench.DATASETS[dataset]
    ydb = dict(dataset=dataset, metric=metric, filtered=False, id_origin=0)
    pg = copy.deepcopy(ydb)
    for split, rows in [('base', 100), ('queries', 10)]:
        common = dict(rows=rows, dimension=dimension, scale=1.0)
        file = dict(bytes=1, sha256='a' * 64)
        ydb[split] = dict(common, files=[dict(file, file='part-00000.parquet', rows=rows,
                                            first_id=0, last_id=rows - 1)])
        pg[split] = dict(common, **file, file=split + '.copy')
    return ydb, pg


def options(directory, dataset='text2image-10M', index='hnsw'):
    return SimpleNamespace(output=str(directory), dataset=dataset, ydb_index=index, threads=2,
                           duration=2, warmup=1, iterations=2, targets=10, limit=10, ef_search=50,
                           levels=1, clusters=10, operation_timeout=100, ydb_bin='ydb', keep_data=False)


class ResultTests(unittest.TestCase):
    def test_ydb_last_total(self):
        output = 'Total Txs Txs/Sec Retries Errors\n1 0 0 0 1\nTotal Txs Txs/Sec Retries Errors\n2 200 100 0 0\n'
        self.assertEqual(bench.parse_ydb(output)['qps'], 100)

    def test_ydb_bad_results(self):
        for row in ['', '1 0 0 0 0', '1 10 10 0 1', '1 10 nan 0 0', '1 10 inf 0 0']:
            with self.subTest(row=row), self.assertRaises(ValueError):
                bench.parse_ydb('Total Txs Txs/Sec Retries Errors\n' + row)

    def test_postgres_results(self):
        summary = 'number of transactions actually processed: 200\nnumber of failed transactions: 0 (0%)\ntps = 100.50 (without initial connection time)\n'
        self.assertEqual(bench.parse_pg(summary)['qps'], 100.5)
        with self.assertRaises(ValueError):
            bench.parse_pg(summary.replace('failed transactions: 0', 'failed transactions: 1'))
        with self.assertRaises(ValueError):
            bench.parse_pg(summary + 'pgbench: error: client 1 aborted\n')
        with self.assertRaises(ValueError):
            bench.parse_pg('no results')

    def test_libpq_uri_credentials(self):
        env, passwords = bench.pg_environment('postgresql://alice:two%20words@localhost:5433/bench?sslmode=require')
        self.assertEqual(env['PGUSER'], 'alice')
        self.assertEqual(env['PGDATABASE'], 'bench')
        self.assertEqual(env['PGPASSWORD'], 'two words')
        self.assertEqual(env['PGSSLMODE'], 'require')
        self.assertEqual(passwords, ['two words'])
        self.assertNotIn('PG_DSN', env)

    def test_libpq_keyword_credentials(self):
        env, _ = bench.pg_environment("host=localhost dbname=bench user=alice password='two words'")
        self.assertEqual(env['PGPASSWORD'], 'two words')

    def test_manifest_checks(self):
        for dataset in bench.DATASETS:
            ydb, pg = manifests(dataset)
            self.assertEqual(bench.validate_manifests(dataset, ydb, pg)[1:], (100, 10))
            for change in ['scale', 'rows', 'path', 'ids']:
                broken = copy.deepcopy(ydb)
                if change == 'path':
                    broken['base']['files'][0]['file'] = '../base.parquet'
                elif change == 'ids':
                    broken['base']['files'][0]['first_id'] = 1
                else:
                    broken['base'][change] += 1
                with self.subTest(dataset=dataset, change=change), self.assertRaises(ValueError):
                    bench.validate_manifests(dataset, broken, pg)


class OrchestrationTests(unittest.TestCase):
    def test_all_datasets_and_index_types(self):
        for dataset in bench.DATASETS:
            for index in ('hnsw', 'vector_kmeans_tree'):
                with self.subTest(dataset=dataset, index=index), tempfile.TemporaryDirectory() as directory:
                    with patch.dict(os.environ, {'PG_DSN': 'host=localhost dbname=bench user=test',
                                                 'YDB_ENDPOINT': 'grpc://localhost:2135', 'YDB_DATABASE': '/Root/test'}):
                        runner = bench.Benchmark(options(directory, dataset, index))
                        runner.dimension = bench.DATASETS[dataset][0]
                        runner.rows, runner.targets = 100, 10
                        calls = []

                        def command(label, argv, **kwargs):
                            calls.append((label, argv, kwargs))
                            if label == 'ydb-table-stats':
                                return json.dumps({'table_stats': {'rows_estimate': '100'}})
                            if label == 'pg-query-ids':
                                return '10\n'
                            if label == 'pg-base-count':
                                return '100\n'
                            if label == 'pg-explain':
                                return '[{"Index Name": "base_embedding_hnsw"}]'
                            if label.startswith('ydb-') and label.endswith(('warmup', 'measure')):
                                return 'Total Txs Txs/Sec Retries Errors\n2 200 100 0 0\n'
                            if label.startswith('postgres-') and label.endswith(('warmup', 'measure')):
                                return 'number of transactions actually processed: 200\ntps = 100 (without initial connection time)\n'
                            return ''

                        runner.command = command
                        runner.prepare(Path(directory))
                        runner.measure(Path(directory))
                        self.assertEqual(runner.results['status'], 'complete')
                        self.assertEqual(len(runner.results['runs']), 4)
                        self.assertEqual([r['backend'] for r in runner.results['runs']], ['ydb', 'postgres', 'postgres', 'ydb'])
                        build = next(c[1] for c in calls if c[0] == 'ydb-build-index')
                        self.assertEqual(build[build.index('--index-type') + 1], index)
                        self.assertEqual('--min-rows' in build, index == 'hnsw')
                        imports = [c[1] for c in calls if c[0].startswith('ydb-import-')]
                        self.assertTrue(all(c[c.index('--bulk-size') + 1] == ('128' if dataset == 'sparse' else '2000') for c in imports))
                        selects = [c[1] for c in calls if c[0].startswith('ydb-') and c[0].endswith('measure')]
                        self.assertTrue(all(('--ef-search' in c) == (index == 'hnsw') for c in selects))
                        self.assertFalse(runner.cleanup())
                        drops = [c[2]['stdin'] for c in calls if c[0] == 'cleanup-postgres']
                        self.assertEqual(drops, [f'DROP SCHEMA {runner.name} CASCADE;'])
                        self.assertTrue(all('PG_DSN' not in c[2].get('env', {}) for c in calls))

    def test_credentials_are_redacted_in_logs_not_results(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'PG_DSN': 'user=test password=1 dbname=bench'}):
            runner = bench.Benchmark(options(directory))
            with patch.object(subprocess, 'run', return_value=SimpleNamespace(returncode=0, stdout='password 1, qps 100')):
                output = runner.command('mock', ['psql'])
            self.assertEqual(output, 'password 1, qps 100')
            self.assertNotIn('1', (Path(directory) / '001-mock.log').read_text())


    def test_s3_download_checksums_and_layout(self):
        for corrupt in (False, True):
            with self.subTest(corrupt=corrupt), tempfile.TemporaryDirectory() as directory:
                with patch.dict(os.environ, {'PG_DSN': 'user=test dbname=bench'}):
                    runner = bench.Benchmark(options(Path(directory) / 'results'))
                runner.args.s3_prefix = 'ann'
                runner.args.s3_bucket = 'bucket'
                runner.args.s3_endpoint = 'https://storage.yandexcloud.net'
                runner.args.s3_unsigned = True
                payload = b'example export data'
                ydb, pg = manifests('text2image-10M')
                objects = {}
                for split in ('base', 'queries'):
                    for item in [pg[split], *ydb[split]['files']]:
                        item['bytes'] = len(payload)
                        item['sha256'] = hashlib.sha256(payload).hexdigest()
                    objects[f'ann/ydb_vector_data/text2image-10M/{split}/part-00000.parquet'] = payload
                    objects[f'ann/pgbench_data/text2image-10M/{split}.copy'] = payload
                for kind, manifest in [('ydb_vector_data', ydb), ('pgbench_data', pg)]:
                    objects[f'ann/{kind}/text2image-10M/manifest.json'] = json.dumps(manifest).encode()
                if corrupt:
                    objects['ann/pgbench_data/text2image-10M/base.copy'] = b'damaged'
                downloaded = []

                def download(bucket, key, filename):
                    self.assertEqual(bucket, 'bucket')
                    downloaded.append(key)
                    Path(filename).write_bytes(objects[key])

                modules = {
                    'boto3': SimpleNamespace(client=lambda *args, **kwargs: SimpleNamespace(download_file=download)),
                    'botocore': SimpleNamespace(UNSIGNED='unsigned'),
                    'botocore.config': SimpleNamespace(Config=lambda **kwargs: kwargs),
                }
                data = Path(directory) / 'downloads'
                data.mkdir()
                with patch.dict(sys.modules, modules):
                    if corrupt:
                        with self.assertRaisesRegex(RuntimeError, 'checksum mismatch'):
                            runner.download(data)
                    else:
                        runner.download(data)
                        self.assertEqual(set(downloaded), set(objects))
                        self.assertEqual(runner.targets, 10)

    def test_cleanup_only_owned_resources(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'PG_DSN': 'user=test dbname=bench'}):
            runner = bench.Benchmark(options(directory))
            with patch.object(runner, 'command') as command:
                self.assertEqual(runner.cleanup(), [])
                command.assert_not_called()
            runner.pg_created = True
            runner.ydb_created = [runner.base]
            runner.args.keep_data = True
            with patch.object(runner, 'command') as command:
                self.assertEqual(runner.cleanup(), [])
                command.assert_not_called()


if __name__ == '__main__':
    unittest.main()
