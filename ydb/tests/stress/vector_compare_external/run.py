#!/usr/bin/env python3
"""Load matching S3 exports and compare YDB vector search with pgvector."""
import argparse
import ctypes
import ctypes.util
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import statistics
import subprocess
import tempfile
import time
import uuid


DATASETS = {
    'text2image-10M': (200, 'ip', 'inner_product', 'vector', 'vector_ip_ops', '<#>'),
    'yfcc-10M': (192, 'l2', 'euclidean', 'vector', 'vector_l2_ops', '<->'),
    'sparse': (30109, 'ip', 'inner_product', 'sparsevec', 'sparsevec_ip_ops', '<#>'),
}


def pg_environment(dsn):
    """Use libpq's parser, including URI escaping, without putting a DSN in argv."""
    class Option(ctypes.Structure):
        _fields_ = [(name, ctypes.c_char_p) for name in
                    ('keyword', 'envvar', 'compiled', 'val', 'label', 'dispchar')] + [('dispsize', ctypes.c_int)]

    lib = ctypes.CDLL(ctypes.util.find_library('pq') or 'libpq.so.5')
    lib.PQconninfoParse.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
    lib.PQconninfoParse.restype = ctypes.POINTER(Option)
    lib.PQconninfoFree.argtypes = [ctypes.POINTER(Option)]
    lib.PQconninfoFree.restype = None
    lib.PQfreemem.argtypes = [ctypes.c_void_p]
    lib.PQfreemem.restype = None
    error = ctypes.c_void_p()
    options = lib.PQconninfoParse(dsn.encode(), ctypes.byref(error))
    if not options:
        if error.value:
            lib.PQfreemem(error)
        raise ValueError('Invalid PG_DSN (libpq could not parse it)')
    env = {k: v for k, v in os.environ.items() if not k.startswith('PG')}
    passwords = []
    try:
        i = 0
        while options[i].keyword:
            item = options[i]
            if item.val is not None:
                if not item.envvar:
                    raise ValueError('PG_DSN option has no environment equivalent: ' + item.keyword.decode())
                env[item.envvar.decode()] = item.val.decode()
                if 'password' in item.keyword.decode():
                    passwords.append(item.val.decode())
            i += 1
    finally:
        lib.PQconninfoFree(options)
    env.setdefault('PGCONNECT_TIMEOUT', '30')
    return env, passwords


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError('must be positive')
    return number


def parse_ydb(output):
    lines = output.splitlines()
    total = None
    for i, line in enumerate(lines):
        if line.startswith('Total'):
            total = dict(zip(line.split(), lines[i + 1].split())) if i + 1 < len(lines) else {}
    try:
        count, errors, qps = int(total['Txs']), int(total['Errors']), float(total['Txs/Sec'])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError('Missing or malformed YDB Total row') from error
    if count <= 0 or errors or not math.isfinite(qps) or qps <= 0:
        raise ValueError('YDB measurement has errors or no successful queries')
    return {'qps': qps, 'transactions': count, 'errors': errors}


def parse_pg(output):
    count = re.search(r'number of transactions actually processed:\s*(\d+)', output)
    failed = re.search(r'number of failed transactions:\s*(\d+)', output)
    rates = re.findall(r'^tps = ([\d.eE+-]+) \((?:without initial connection time|excluding connections establishing)\)', output, re.M)
    if not count or not rates:
        raise ValueError('Missing pgbench transaction count or TPS summary')
    qps = float(rates[-1])
    errors = int(failed[1]) if failed else 0  # Older pgbench versions omit zero failures.
    if int(count[1]) <= 0 or errors or not math.isfinite(qps) or qps <= 0 or re.search(r'\b(?:ERROR|FATAL)\b|client .* aborted', output):
        raise ValueError('PostgreSQL measurement has errors or no successful queries')
    return {'qps': qps, 'transactions': int(count[1]), 'errors': errors}


def validate_manifests(dataset, ydb, pg):
    dimension, metric, *_ = DATASETS[dataset]
    for manifest in (ydb, pg):
        if manifest.get('dataset') != dataset or manifest.get('metric') != metric or manifest.get('filtered') is not False:
            raise ValueError('Dataset identity, metric, or filtering differs from the requested benchmark')
    for split in ('base', 'queries'):
        a, b = ydb[split], pg[split]
        if a['rows'] != b['rows'] or int(a['rows']) <= 0 or a['dimension'] != dimension or b['dimension'] != dimension:
            raise ValueError('YDB and PostgreSQL row counts or dimensions differ')
        if a['scale'] != b['scale']:
            raise ValueError('YDB and PostgreSQL vector scales differ')
        if b['file'] != split + '.copy':
            raise ValueError('Unexpected PostgreSQL COPY filename')
        if not a['files'] or sum(f['rows'] for f in a['files']) != a['rows']:
            raise ValueError('Invalid Parquet file row counts')
        next_id = 0
        names = set()
        for file in a['files']:
            if not re.fullmatch(r'part-[0-9]+\.parquet', file['file']):
                raise ValueError('Unsafe or unexpected Parquet filename')
            if file['file'] in names or file['rows'] <= 0:
                raise ValueError('Duplicate filenames or invalid Parquet row counts')
            names.add(file['file'])
            if file['first_id'] != next_id or file['last_id'] + 1 != next_id + file['rows']:
                raise ValueError('Query/base IDs must be contiguous and zero-based')
            next_id += file['rows']
        for file in [b, *a['files']]:
            if not re.fullmatch(r'[0-9a-f]{64}', file['sha256']) or int(file['bytes']) <= 0:
                raise ValueError('Invalid file checksum or size')
    if ydb.get('id_origin') != 0 or pg.get('id_origin') != 0:
        raise ValueError('Only zero-based exports are supported')
    return dimension, int(ydb['base']['rows']), int(ydb['queries']['rows'])


class Benchmark:
    def __init__(self, args):
        self.args = args
        self.output = Path(args.output).resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.pg_env, passwords = pg_environment(os.environ['PG_DSN'])
        self.secrets = [v for v in [os.environ['PG_DSN'], os.getenv('YDB_TOKEN'),
                                  os.getenv('AWS_SECRET_ACCESS_KEY'), os.getenv('AWS_SESSION_TOKEN'), *passwords] if v]
        self.pg_env['PGOPTIONS'] = self.pg_env.get('PGOPTIONS', '') + f' -c hnsw.ef_search={args.ef_search}'
        self.name = 'ann_' + args.dataset.replace('-', '_') + '_' + uuid.uuid4().hex[:12]
        self.base, self.queries = self.name + '_base', self.name + '_queries'
        self.index = 'vidx_' + args.ydb_index
        self.ydb_created = []
        self.pg_created = False
        self.counter = 0
        self.results = {'dataset': args.dataset, 'ydb_index': args.ydb_index, 'namespace': self.name,
                        'status': 'incomplete', 'settings': {
                            key: getattr(args, key) for key in ('threads', 'duration', 'warmup', 'iterations',
                                                               'targets', 'limit', 'ef_search', 'levels', 'clusters')},
                        'runs': [], 'notes': [
                            'Recall is not measured; equal search quality is not established.',
                            'YDB loads query vectors before timing; pgbench includes a query-table lookup per search.',
                            'YDB cycles over query IDs; pgbench samples the same ID range with replacement.',
                        ]}
        if args.dataset == 'sparse':
            self.results['notes'].append('YDB uses dense float32(30109); PostgreSQL uses sparsevec(30109).')

    def redact(self, text):
        for secret in sorted(self.secrets, key=len, reverse=True):
            text = text.replace(secret, '***')
        return text

    def command(self, label, command, *, env=None, stdin=None, timeout=None):
        self.counter += 1
        print(label, flush=True)
        logfile = self.output / f'{self.counter:03d}-{label}.log'
        try:
            result = subprocess.run(command, input=stdin, text=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, env=env, timeout=timeout or self.args.operation_timeout)
        except subprocess.TimeoutExpired as error:
            output = error.stdout or b''
            logfile.write_text(self.redact(output.decode(errors='replace') if isinstance(output, bytes) else output))
            raise RuntimeError(f'{label} timed out; see {logfile.name}') from None
        output = self.redact(result.stdout)
        logfile.write_text(output)
        if result.returncode:
            raise RuntimeError(f'{label} failed (exit {result.returncode}); see {logfile.name}')
        return result.stdout

    def ydb(self, label, args, timeout=None):
        return self.command(label, [self.args.ydb_bin, '-e', os.environ['YDB_ENDPOINT'],
                                   '-d', os.environ['YDB_DATABASE'], *args], timeout=timeout)

    def pg(self, label, sql):
        return self.command(label, ['psql', '-X', '-w', '-v', 'ON_ERROR_STOP=1', '-Atq'], env=self.pg_env, stdin=sql)

    def download(self, directory):
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config
        config = Config(signature_version=UNSIGNED) if self.args.s3_unsigned else Config()
        client = boto3.client('s3', endpoint_url=self.args.s3_endpoint, region_name='ru-central1', config=config)
        prefix = self.args.s3_prefix.strip('/')
        def fetch(key, destination):
            key = '/'.join(p for p in (prefix, key) if p)
            destination.parent.mkdir(parents=True, exist_ok=True)
            try:
                client.download_file(self.args.s3_bucket, key, str(destination))
            except Exception:
                raise RuntimeError(f'S3 download failed: {key}') from None
        manifests = []
        for kind in ('ydb_vector_data', 'pgbench_data'):
            path = directory / kind / self.args.dataset / 'manifest.json'
            fetch(f'{kind}/{self.args.dataset}/manifest.json', path)
            manifests.append(json.loads(path.read_text()))
        dimension, rows, queries = validate_manifests(self.args.dataset, *manifests)
        for kind, manifest in zip(('ydb', 'postgres'), manifests):
            safe_manifest = {k: v for k, v in manifest.items() if k != 'sources'}
            (self.output / (kind + '-manifest.json')).write_text(json.dumps(safe_manifest, indent=2) + '\n')
        self.results.update(dimension=dimension, base_rows=rows, query_rows=queries)
        self.targets = min(self.args.targets, queries)
        self.results['settings']['targets'] = self.targets
        files = []
        for split in ('base', 'queries'):
            for item in manifests[0][split]['files']:
                files.append((f'ydb_vector_data/{self.args.dataset}/{split}/{item["file"]}', item))
            item = manifests[1][split]
            files.append((f'pgbench_data/{self.args.dataset}/{item["file"]}', item))
        required = sum(int(item['bytes']) for _, item in files)
        if shutil.disk_usage(directory).free < required + 1024 ** 3:
            raise RuntimeError('Not enough free disk space for the selected S3 exports')
        for key, item in files:
            path = directory / key
            print('Downloading and verifying ' + key, flush=True)
            fetch(key, path)
            digest = hashlib.sha256()
            with path.open('rb') as source:
                for chunk in iter(lambda: source.read(8 * 1024 * 1024), b''):
                    digest.update(chunk)
            if path.stat().st_size != item['bytes'] or digest.hexdigest() != item['sha256']:
                raise RuntimeError('S3 file size/checksum mismatch: ' + key)
        # Manifests contain source paths; keep only benchmark metadata in artifacts.
        self.dimension, self.rows = dimension, rows

    def prepare(self, directory):
        _, _, distance, pg_type, opclass, _ = DATASETS[self.args.dataset]
        self.ydb('ydb-version', ['version', '--semantic'])
        self.pg('pg-version', "SELECT version(); SELECT extversion FROM pg_extension WHERE extname='vector';")
        self.pg('pg-create-schema', f'CREATE SCHEMA {self.name};')
        self.pg_created = True
        self.pg('pg-create-tables', f'''CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE {self.name}.base (id bigint NOT NULL, embedding {pg_type}({self.dimension}) NOT NULL);
CREATE TABLE {self.name}.queries (id bigint NOT NULL, embedding {pg_type}({self.dimension}) NOT NULL);''')
        for split, table in [('base', self.base), ('queries', self.queries)]:
            self.ydb('ydb-init-' + split, ['workload', 'vector', 'init', '--table', table])
            self.ydb_created.append(table)
            self.ydb('ydb-import-' + split, [
                'workload', 'vector', 'import', '--upload-threads', '4', '--max-in-flight', '4',
                '--bulk-size', '128' if self.args.dataset == 'sparse' else '2000',
                'files', '--input', str(directory / 'ydb_vector_data' / self.args.dataset / split),
                '--format', 'parquet', '--table', table, '--vector-type', 'float',
                '--vector-dimension', str(self.dimension), '--distance', distance, '--index-type', 'None'])
            path = str(directory / 'pgbench_data' / self.args.dataset / (split + '.copy')).replace("'", "''")
            self.pg('pg-import-' + split, f"\\copy {self.name}.{split} (id, embedding) FROM '{path}' WITH (FORMAT binary)\n")
            self.pg('pg-primary-key-' + split, f'ALTER TABLE {self.name}.{split} ADD PRIMARY KEY (id);')
        # Avoid building an automatically sized index from stale statistics.
        deadline = time.monotonic() + 300
        while True:
            desc = json.loads(self.ydb('ydb-table-stats', ['scheme', 'describe', self.base, '--stats', '--format', 'proto-json-base64']))
            stats = desc.get('table_stats', desc.get('tableStats', {}))
            if int(stats.get('rows_estimate', stats.get('rowsEstimate', 0))) >= self.rows:
                break
            if time.monotonic() > deadline:
                raise RuntimeError('YDB table statistics did not catch up after import')
            time.sleep(5)
        args = ['workload', 'vector', 'build-index', '--table', self.base, '--index', self.index,
                '--index-type', self.args.ydb_index, '--vector-type', 'float', '--vector-dimension', str(self.dimension),
                '--distance', distance, '--kmeans-tree-levels', str(self.args.levels),
                '--kmeans-tree-clusters', str(self.args.clusters)]
        if self.args.ydb_index == 'hnsw':
            args += ['--min-rows', '1', '--M', '16', '--ef-construction', '200', '--delta-rows', '10000']
        self.ydb('ydb-build-index', args)
        self.pg('pg-build-index', f'''CREATE INDEX base_embedding_hnsw ON {self.name}.base
USING hnsw (embedding {opclass}) WITH (m=16, ef_construction=200);
ANALYZE {self.name}.base; ANALYZE {self.name}.queries;''')
        self.ydb('ydb-index-description', ['scheme', 'describe', self.base, '--stats', '--format', 'proto-json-base64'])
        # Check that the requested query IDs exist, before timing any workload.
        count = self.pg('pg-query-ids', f'SELECT count(*) FROM {self.name}.queries WHERE id >= 0 AND id < {self.targets};')
        if int(count.strip()) != self.targets:
            raise RuntimeError('PostgreSQL query ID range is incomplete')
        count = self.pg('pg-base-count', f'SELECT count(*) FROM {self.name}.base;')
        if int(count.strip()) != self.rows:
            raise RuntimeError('PostgreSQL base row count differs from the export manifest')

    def measure(self, directory):
        operator = DATASETS[self.args.dataset][-1]
        sql = (f'SELECT b.id FROM {self.name}.base AS b ORDER BY b.embedding {operator} '
               f'(SELECT q.embedding FROM {self.name}.queries AS q WHERE q.id = :qid) LIMIT :k;\n')
        script = directory / 'pgbench.sql'
        script.write_text(f'\\set qid random(0, {self.targets - 1})\n' + sql)
        plan = self.pg('pg-explain', 'EXPLAIN (FORMAT JSON) ' + sql.replace(':qid', '0').replace(':k', str(self.args.limit)))
        if 'base_embedding_hnsw' not in plan:
            raise RuntimeError('PostgreSQL query plan does not use the HNSW index')
        for iteration in range(1, self.args.iterations + 1):
            # Alternate execution order to reduce systematic cache/load ordering bias.
            for backend in (('ydb', 'postgres') if iteration % 2 else ('postgres', 'ydb')):
                for phase, seconds in [('warmup', self.args.warmup), ('measure', self.args.duration)]:
                    label = f'{backend}-{iteration}-{phase}'
                    if backend == 'ydb':
                        args = ['workload', 'vector', 'run', 'select', '--table', self.base, '--index', self.index,
                                '--query-table', self.queries, '--targets', str(self.targets), '--limit', str(self.args.limit),
                                '--threads', str(self.args.threads), '--seconds', str(seconds), '--client-timeout', '30s']
                        if self.args.ydb_index == 'hnsw':
                            args += ['--ef-search', str(self.args.ef_search)]
                        output = self.ydb(label, args, timeout=seconds + 300)
                        result = parse_ydb(output)
                    else:
                        output = self.command(label, ['pgbench', '-n', '-M', 'prepared', '-c', str(self.args.threads),
                                                     '-j', str(self.args.threads), '-T', str(seconds), '-P', '5',
                                                     '-D', f'k={self.args.limit}', '-f', str(script)], env=self.pg_env,
                                              timeout=seconds + 300)
                        result = parse_pg(output)
                    if phase == 'measure':
                        self.results['runs'].append(dict(backend=backend, iteration=iteration, **result))
                        self.report()
        self.results['status'] = 'complete'

    def cleanup(self):
        errors = []
        if self.args.keep_data:
            return errors
        for table in reversed(self.ydb_created):
            try:
                self.ydb('cleanup-' + table, ['yql', '-s', f'DROP TABLE `{table}`;'], timeout=120)
            except Exception:
                errors.append('Could not remove YDB table ' + table)
        if self.pg_created:
            try:
                self.pg('cleanup-postgres', f'DROP SCHEMA {self.name} CASCADE;')
            except Exception:
                errors.append('Could not remove PostgreSQL schema ' + self.name)
        return errors

    def report(self):
        (self.output / 'results.json').write_text(json.dumps(self.results, indent=2) + '\n')
        lines = [f'# Vector benchmark: {self.args.dataset}', '', f'Status: **{self.results["status"]}**', '',
                 f'YDB index: `{self.args.ydb_index}`. Resource prefix: `{self.name}`.', '',
                 'Settings: `' + json.dumps(self.results['settings'], sort_keys=True) + '`', '',
                 '| Backend | QPS by iteration | Median QPS |', '|---|---|---|']
        if 'base_rows' in self.results:
            lines[4:4] = [f'Base rows: {self.results["base_rows"]:,}; query rows: {self.results["query_rows"]:,}; '
                          f'dimensions: {self.results["dimension"]}.', '']
        medians = {}
        for backend in ('ydb', 'postgres'):
            values = [run['qps'] for run in self.results['runs'] if run['backend'] == backend]
            if values:
                medians[backend] = statistics.median(values)
                lines.append(f'| {backend} | ' + ', '.join(f'{v:.2f}' for v in values) + f' | {medians[backend]:.2f} |')
        if self.results['status'] == 'complete' and len(medians) == 2:
            lines += ['', f'YDB / PostgreSQL median throughput: **{medians["ydb"] / medians["postgres"]:.3f}×**.']
        lines += ['', *self.results['notes']]
        if self.results.get('error'):
            lines += ['', 'Error: ' + self.results['error']]
        if self.results.get('cleanup_errors'):
            lines += ['', *self.results['cleanup_errors']]
        (self.output / 'report.md').write_text('\n'.join(lines) + '\n')


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', choices=DATASETS, required=True)
    parser.add_argument('--ydb-index', choices=['hnsw', 'vector_kmeans_tree'], default='hnsw')
    parser.add_argument('--ydb-bin', required=True)
    parser.add_argument('--s3-endpoint', default='https://storage.yandexcloud.net')
    parser.add_argument('--s3-bucket', required=True)
    parser.add_argument('--s3-prefix', default='')
    parser.add_argument('--s3-unsigned', action='store_true')
    parser.add_argument('--output', required=True)
    parser.add_argument('--keep-data', action='store_true')
    for name, default in [('threads', 50), ('duration', 100), ('warmup', 60), ('iterations', 3),
                          ('targets', 1000), ('limit', 10), ('ef-search', 50), ('levels', 1), ('clusters', 10),
                          ('operation-timeout', 86400)]:
        parser.add_argument('--' + name, type=positive, default=default)
    args = parser.parse_args()
    if args.ef_search < args.limit:
        parser.error('ef-search must be at least limit for the PostgreSQL HNSW search')
    if args.ef_search > 1000 or args.threads > 1024:
        parser.error('ef-search must be <= 1000 and threads <= 1024')
    for name in ('PG_DSN', 'YDB_ENDPOINT', 'YDB_DATABASE'):
        if not os.getenv(name):
            parser.error('Set ' + name)
    for executable in ('psql', 'pgbench', args.ydb_bin):
        if not shutil.which(executable):
            parser.error('Executable not found: ' + executable)
    return args


def main():
    args = arguments()
    benchmark = Benchmark(args)
    benchmark.report()
    def terminate(signum, frame):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, terminate)
    try:
        with tempfile.TemporaryDirectory(prefix='vector-compare-', dir=os.getenv('RUNNER_TEMP')) as directory:
            benchmark.download(Path(directory))
            benchmark.prepare(Path(directory))
            benchmark.measure(Path(directory))
    except (Exception, KeyboardInterrupt) as error:
        benchmark.results['status'] = 'failed'
        benchmark.results['error'] = benchmark.redact(str(error)) or 'Interrupted'
    finally:
        benchmark.results['cleanup_errors'] = benchmark.cleanup()
        benchmark.report()
    return 0 if benchmark.results['status'] == 'complete' and not benchmark.results['cleanup_errors'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
