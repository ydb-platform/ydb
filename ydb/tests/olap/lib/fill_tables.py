"""Prepare benchmark tables and import their parts from static YDB nodes."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass, replace
from decimal import Decimal
import json
import logging
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
from tempfile import TemporaryDirectory
from time import sleep
from urllib.parse import urlsplit
from uuid import uuid4

import requests

from .compaction import force_datashard_compact_legacy, force_datashard_compact_sdk, wait_column_compaction
from .remote_execution import LongRemoteExecution, copy_file, execute_command, is_localhost
from .ydb_cluster import YdbCluster


LOGGER = logging.getLogger(__name__)
SANDBOX_RESOURCE_API = 'https://sandbox.yandex-team.ru/api/v1.0/resource'


@dataclass(frozen=True)
class Dataset:
    workload: str
    path: str
    scale: str | int | float = '1'
    init_scale: str = ''
    store: str = 'column'
    float_mode: str = ''
    datetime_types: str = 'dt64'
    clear: bool = False
    tables: str = ''
    input_path: str = ''
    suite_path: str = ''
    import_options: tuple[str, ...] = ()

    @property
    def command(self) -> list[str]:
        workload = 'query' if self.workload.startswith('external_') else self.workload
        return ['workload', workload, '-p', self.path]

    @property
    def uses_generator(self) -> bool:
        return self.workload in ('tpch', 'tpcds')

    @property
    def needs_resources(self) -> bool:
        return not (self.uses_generator or self.input_path or self.workload == 'query')


@dataclass(frozen=True)
class Worker:
    host: str
    endpoint: str
    directory: str
    cli: str
    auth: tuple[str, ...]
    remote: bool


def find_resources(workload: str, count: int) -> list[dict]:
    headers = {}
    if os.getenv('SANDBOX_TOKEN'):
        headers['Authorization'] = f"OAuth {os.environ['SANDBOX_TOKEN']}"
    resources = []
    for part in range(count):
        response = requests.get(SANDBOX_RESOURCE_API, headers=headers, timeout=30, params={
            'type': 'OTHER_RESOURCE', 'state': 'READY', 'limit': 1, 'order': '-id',
            'attrs': json.dumps({'benchmark_type': workload, 'part': str(part)}),
        })
        response.raise_for_status()
        items = response.json()['items']
        if not items or not items[0].get('id'):
            raise RuntimeError(f'No downloadable Sandbox resource for {workload}, part {part}')
        resources.append(items[0])
    return resources


class TableFiller:
    def __init__(self, cli: str, *, static_nodes=None, key_file='', token_file='', resource_parts=16, attempts=3):
        self.cli = os.path.abspath(cli)
        self.static_nodes = static_nodes
        self.database = '/' + YdbCluster.ydb_database.strip('/')
        self.auth = ()
        if key_file:
            self.auth = ('--sa-key-file', os.path.abspath(key_file))
        elif token_file:
            self.auth = ('--token-file', os.path.abspath(token_file))
        if resource_parts < 1 or attempts < 1:
            raise ValueError('Resource part count and import attempts must be positive')
        self.resource_parts = resource_parts
        self.attempts = attempts

    @staticmethod
    def _discover_static_nodes() -> dict[str, str]:
        if YdbCluster.ydb_mon_port == 0:
            return {'localhost': YdbCluster.ydb_endpoint}
        scheme = 'grpcs' if YdbCluster.ydb_endpoint.startswith('grpcs://') else 'grpc'
        nodes = YdbCluster.get_cluster_nodes(role=YdbCluster.Node.Role.STORAGE, db_only=False)
        host_ports = sorted(
            (node.host, node.grpcs_port if scheme == 'grpcs' else node.grpc_port)
            for node in nodes if node.host and not node.disconnected
        )
        endpoints = {}
        for host, port in host_ports:
            if port:
                address = f'[{host}]' if ':' in host else host
                endpoints.setdefault(host, f'{scheme}://{address}:{port}')
        return endpoints

    def __enter__(self):
        if not os.access(self.cli, os.X_OK) or not os.path.isfile(self.cli):
            raise ValueError(f'YDB CLI is not an executable file: {self.cli}')
        endpoints = self.static_nodes or self._discover_static_nodes()
        if not endpoints:
            raise RuntimeError('No connected static nodes with a gRPC endpoint')
        with ExitStack() as stack:
            self.workers = []
            for host, endpoint in endpoints.items():
                remote = not is_localhost(host)
                target = host if remote else 'localhost'
                directory = execute_command(
                    target, ['mktemp', '-d', '/tmp/ydb-fill-tables.XXXXXXXX'], timeout=30,
                ).stdout.strip()
                if not re.fullmatch(r'/tmp/ydb-fill-tables\.[A-Za-z0-9]{8}', directory):
                    raise RuntimeError(f'Unexpected temporary directory on {host}: {directory!r}')
                stack.callback(self._cleanup, target, directory)
                cli, auth = self.cli, self.auth
                if remote:
                    cli = f'{directory}/ydb'
                    copy_file(self.cli, host, cli, prepare_destination=False)
                    execute_command(host, ['chmod', 'u+x', cli], timeout=30)
                    if auth:
                        key_path = f'{directory}/credentials'
                        copy_file(auth[1], host, key_path, prepare_destination=False)
                        auth = (auth[0], key_path)
                self.workers.append(Worker(host, endpoint, directory, cli, auth, remote))
            self._stack = stack.pop_all()
        return self

    def __exit__(self, *exc):
        self._stack.close()

    @staticmethod
    def _cleanup(host, directory):
        try:
            result = execute_command(host, ['rm', '-rf', '--', directory], raise_on_error=False, timeout=30)
            if result.exit_code != 0:
                LOGGER.warning('Failed to remove %s on %s', directory, host or 'localhost')
        except (OSError, subprocess.TimeoutExpired):
            LOGGER.warning('Failed to remove %s on %s', directory, host or 'localhost', exc_info=True)

    def init(self, dataset: Dataset):
        if not dataset.store:
            return
        command = [self.cli, '-e', YdbCluster.ydb_endpoint, '-d', self.database,
                   *self.auth, *dataset.command, 'init']
        if dataset.suite_path:
            command += ['--suite-path', dataset.suite_path]
        else:
            command += ['--store', dataset.store]
            if dataset.datetime_types:
                command += ['--datetime-types', dataset.datetime_types]
            if dataset.uses_generator:
                if dataset.init_scale:
                    command += ['--scale', dataset.init_scale]
                if dataset.float_mode:
                    command += ['--float-mode', dataset.float_mode]
        if dataset.clear:
            command.append('--clear')
        execute_command('localhost', command)

    @staticmethod
    def _stage_query_suite(suite_path, data_path, directory, host, remote):
        if not remote and not data_path:
            return suite_path

        source = Path(suite_path).absolute()
        staged = f'{directory}/suite'
        if not data_path:
            copy_file(str(source), host, staged, prepare_destination=False)
            return staged

        def exclude_bundled_data(path, names):
            return {'import'} if Path(path) == source and 'import' in names else set()

        execute_command(host, ['test', '-d', data_path], timeout=30)
        if remote:
            with TemporaryDirectory() as temp:
                prepared = shutil.copytree(source, Path(temp) / 'suite', ignore=exclude_bundled_data)
                copy_file(str(prepared), host, staged, prepare_destination=False)
        else:
            shutil.copytree(source, staged, ignore=exclude_bundled_data)
        execute_command(host, ['ln', '-sT', '--', data_path, f'{staged}/import'], timeout=30)
        return staged

    def _import_part(self, dataset, worker, index, part, count, resource):
        host = worker.host if worker.remote else 'localhost'
        directory = f'{worker.directory}/dataset-{index}/part-{part}'
        execute_command(host, ['mkdir', '-p', directory], timeout=30)
        command = [worker.cli, '-e', worker.endpoint, '-d', self.database, *worker.auth,
                   *dataset.command, 'import', *dataset.import_options]
        if resource:
            resource_id = resource['id']
            name = Path(resource.get('file_name') or '').name
            data_path = f'{directory}/data/{name or resource_id}'
            if resource.get('multifile'):
                # ya download materializes multifile resources as directories.
                data_path = f'{directory}/data'
                output_options = ['--output', data_path]
            elif name.endswith(('.tar', '.tar.gz', '.tgz', '.tar.zst')):
                data_path = f'{directory}/data'
                output_options = ['--untar', '--output', data_path]
            else:
                output_options = ['--output', data_path]
            execute_command(host, ['ya', 'download', *output_options, f'sbr:{resource_id}'], timeout=7200)
        else:
            data_path = os.path.abspath(dataset.input_path) if dataset.input_path else ''
            if data_path and worker.remote:
                data_path = f'{directory}/input/{Path(dataset.input_path).name}'
                execute_command(host, ['mkdir', '-p', f'{directory}/input'], timeout=30)
                copy_file(dataset.input_path, host, data_path, prepare_destination=False)

        if dataset.uses_generator:
            command += ['generator', '--scale', str(dataset.scale), '-C', str(count), '-i', str(part)]
        elif dataset.workload == 'clickbench':
            command += ['files', '--input', data_path]
        else:
            suite_path = self._stage_query_suite(dataset.suite_path, data_path, directory, host, worker.remote)
            command += ['--suite-path', suite_path]
        if dataset.tables:
            command += ['--tables', dataset.tables]
        command += ['--state', f'{directory}/state.json']
        for attempt in range(1, self.attempts + 1):
            LOGGER.info('Import %s, part %s/%s on %s (attempt %s)',
                        dataset.path, part + 1, count, worker.host, attempt)
            if worker.remote:
                with LongRemoteExecution(host, *command) as execution:
                    while execution.is_running():
                        sleep(5)
                    return_code = execution.return_code
                    if return_code != 0:
                        LOGGER.warning('Import command failed on %s (exit %s): %s', host, return_code, execution.stderr)
            else:
                return_code = execute_command('localhost', command, raise_on_error=False).exit_code
            if return_code == 0:
                if resource:
                    self._cleanup(host, directory)
                return
            if attempt < self.attempts:
                sleep(5)
        raise RuntimeError(f'Import of {dataset.path} part {part} on {worker.host} failed: exit {return_code}')

    def _dataset_path(self, path: str) -> PurePosixPath:
        if not isinstance(path, str) or not path.strip('/'):
            raise ValueError('Specify a non-empty dataset table path')
        if path.startswith('/'):
            full_path = '/' + path.lstrip('/')
        else:
            full_path = YdbCluster.get_full_tables_path(path, use_tables_prefix=False)
        absolute = PurePosixPath(full_path)
        if not absolute.is_relative_to(self.database) or absolute == PurePosixPath(self.database) or '..' in absolute.parts:
            raise ValueError(f'Dataset path {path!r} must identify a table or folder under {self.database}')
        return absolute

    def _fill(self, dataset, index, resources):
        count = len(resources) if resources else 1
        if dataset.uses_generator:
            count = len(self.workers)
            if dataset.workload == 'tpcds':
                scale = Decimal(str(dataset.scale))
                if scale != scale.to_integral_value():
                    # TPC-DS partitions the rounded-up scale before truncating row counts.
                    count = 1
        with ThreadPoolExecutor(max_workers=min(count, len(self.workers))) as executor:
            # Keep one active import per worker for this dataset, even when there are more resource parts.
            def import_parts(worker_index, worker):
                for part in range(worker_index, count, len(self.workers)):
                    self._import_part(dataset, worker, index, part, count, resources[part] if resources else None)

            jobs = [executor.submit(import_parts, i, worker) for i, worker in enumerate(self.workers[:count])]
            for job in jobs:
                job.result()

    def _wait_compaction(self, dataset: Dataset, timeout: float, datashard_compaction: str):
        cli = [self.cli, '-e', YdbCluster.ydb_endpoint, '-d', self.database, *self.auth]
        path = str(self._dataset_path(dataset.path))
        entries = json.loads(execute_command(
            'localhost', cli + ['scheme', 'ls', '--format', 'json', '-l', '-R', path],
            timeout=60,
        ).stdout)
        tables = [(path, entries['type'])] if isinstance(entries, dict) else [
            (f'{path}/{entry["path"]}', entry['type']) for entry in entries
        ]
        row_tables = [table for table, kind in tables if kind == 'table']
        column_tables = [table for table, kind in tables if kind == 'column-table']
        if row_tables:
            LOGGER.info('Compacting %d DataShard tables under %s using %s', len(row_tables), path, datashard_compaction)
            if datashard_compaction == 'legacy':
                if not YdbCluster.ydb_mon_port:
                    raise ValueError('Legacy DataShard compaction requires a monitoring port')
                force_datashard_compact_legacy(row_tables, timeout)
            elif self.auth:
                option, credentials_path = self.auth
                oauth = Path(credentials_path).read_text().strip() if option == '--token-file' else None
                iam_file = credentials_path if option == '--sa-key-file' else None
                driver = YdbCluster._create_ydb_driver(YdbCluster.ydb_endpoint, self.database, oauth=oauth, iam_file=iam_file)
                try:
                    force_datashard_compact_sdk(driver, row_tables, timeout)
                finally:
                    driver.stop()
            else:
                force_datashard_compact_sdk(YdbCluster.get_ydb_driver(), row_tables, timeout)

        if not column_tables:
            return

        def execute_query(query, remaining):
            result = execute_command(
                'localhost', cli + ['sql', '--format', 'json-unicode-array', '-s', query],
                timeout=min(60, remaining),
            )
            return json.loads(result.stdout)

        LOGGER.info('Waiting for compaction of %s (%d column tables)', path, len(column_tables))
        wait_column_compaction(column_tables, execute_query, timeout=timeout, interval=5)
        LOGGER.info('Compaction complete for %s', path)

    def fill(self, datasets: list[Dataset], *, initialize=True, compaction_timeout=3600, datashard_compaction='sdk'):
        if datashard_compaction not in ('sdk', 'legacy'):
            raise ValueError(f'Unknown DataShard compaction method: {datashard_compaction}')
        if compaction_timeout < 0:
            raise ValueError('Compaction timeout must be nonnegative')
        normalized = [(dataset, self._dataset_path(dataset.path)) for dataset in datasets]
        paths = sorted(path.parts for _, path in normalized)
        if not paths or any(b[:len(a)] == a for a, b in zip(paths, paths[1:])):
            raise ValueError('Specify datasets with distinct, non-overlapping table paths')
        datasets = [replace(dataset, path=str(path.relative_to(self.database))) for dataset, path in normalized]
        resources = {
            workload: find_resources(workload, self.resource_parts)
            for workload in sorted({d.workload for d in datasets if d.needs_resources})
        }
        for worker in self.workers[:self.resource_parts] if resources else ():
            execute_command(worker.host if worker.remote else 'localhost', ['ya', 'download', '--help'], timeout=30)
        # A new namespace also allows successive fills using the same deployed CLI.
        run_id = uuid4().hex
        with ThreadPoolExecutor(max_workers=len(datasets)) as executor:
            jobs = []
            for index, dataset in enumerate(datasets):
                if initialize:
                    # CLI init --clear can remove empty parent directories shared by datasets.
                    self.init(dataset)
                dataset_resources = resources[dataset.workload] if dataset.needs_resources else None
                jobs.append(executor.submit(self._fill, dataset, f'{run_id}-{index}', dataset_resources))
            for job in jobs:
                job.result()
            if compaction_timeout:
                jobs = [executor.submit(self._wait_compaction, dataset, compaction_timeout, datashard_compaction) for dataset in datasets]
                for job in jobs:
                    job.result()


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cli', required=True, help='Path to the built YDB CLI')
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--database', required=True)
    parser.add_argument('--mon-port', type=int, default=8765)
    parser.add_argument('--static-node', action='append', default=[], metavar='HOST[=ENDPOINT]',
                        help='SSH host with optional gRPC endpoint. Repeat for explicit static nodes')
    parser.add_argument('--resource-parts', type=int, default=16)
    parser.add_argument('--attempts', type=int, default=3)
    parser.add_argument('--compaction-timeout', type=int, default=3600,
                        help='Seconds to wait for table compaction (default: 3600)')
    parser.add_argument('--datashard-compaction', choices=('sdk', 'legacy'), default='sdk',
                        help='DataShard compaction method (default: sdk)')
    auth = parser.add_mutually_exclusive_group()
    auth.add_argument('--sa-key-file', default='')
    auth.add_argument('--token-file', default='')
    parser.add_argument('--dataset', action='append', required=True, type=json.loads,
                        help='JSON object with workload and path, plus dataset options such as scale, store, clear and suite_path')
    args = parser.parse_args()
    static_nodes = {}
    for spec in args.static_node:
        host, _, endpoint = spec.partition('=')
        if not re.fullmatch(r'[A-Za-z0-9:][A-Za-z0-9.:%_-]*', host) or host in static_nodes:
            parser.error(f'Invalid or duplicate static node: {host}')
        if endpoint:
            url = urlsplit(endpoint)
            if url.scheme not in ('grpc', 'grpcs') or not url.hostname:
                parser.error(f'Expected a grpc:// or grpcs:// endpoint for {host}')
        static_nodes[host] = endpoint or args.endpoint
    if args.compaction_timeout <= 0:
        parser.error('Compaction timeout must be positive')
    if args.mon_port < 0:
        parser.error('Monitoring port must be nonnegative')
    try:
        datasets = [Dataset(**spec) for spec in args.dataset]
    except TypeError as error:
        parser.error(f'Invalid dataset: {error}')
    for dataset in datasets:
        if dataset.workload not in ('tpch', 'tpcds', 'clickbench', 'query',
                                    'external_a1', 'external_x1', 'external_m1', 'external_b1'):
            parser.error(f'Unsupported workload: {dataset.workload}')
        if not isinstance(dataset.path, str) or not dataset.path.strip('/') or dataset.store not in ('', 'column', 'row'):
            parser.error(f'Invalid path or store for {dataset.workload}')
        if dataset.command[1] == 'query':
            suite = Path(dataset.suite_path)
            if not dataset.suite_path or not suite.is_dir() or (dataset.store and not (suite / 'init').is_dir()):
                parser.error(f'Specify a valid suite_path for {dataset.workload}')
    YdbCluster.reset(args.endpoint, args.database.lstrip('/'), args.mon_port, None)
    with TableFiller(args.cli, static_nodes=static_nodes, key_file=args.sa_key_file, token_file=args.token_file,
                     resource_parts=args.resource_parts, attempts=args.attempts) as filler:
        filler.fill(datasets, compaction_timeout=args.compaction_timeout, datashard_compaction=args.datashard_compaction)
