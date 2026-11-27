import shutil
import subprocess
from pathlib import Path

import pytest

from ydb.tests.olap.lib.remote_execution import ExecutionResult

from ydb.tests.olap.lib.fill_tables import TableFiller
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


def _node(host, disconnected=False, **ports):
    return YdbCluster.Node({
        'Disconnected': disconnected,
        'SystemState': {
            'Host': host,
            'Roles': ['Storage'],
            'Endpoints': [{'Name': name, 'Address': f':{port}'} for name, port in ports.items()],
        },
    })


@pytest.mark.parametrize(('scheme', 'expected'), [
    ('grpc', {'dual': 'grpc://dual:2135', 'plain-only': 'grpc://plain-only:3135'}),
    ('grpcs', {'dual': 'grpcs://dual:2136', '2001:db8::1': 'grpcs://[2001:db8::1]:3136'}),
])
def test_discover_static_nodes_uses_matching_port(monkeypatch, scheme, expected):
    nodes = [
        _node('dual', grpc=2135, grpcs=2136),
        _node('plain-only', grpc=3135),
        _node('2001:db8::1', grpcs=3136),
        _node('disconnected', disconnected=True, grpc=4135, grpcs=4136),
    ]
    monkeypatch.setattr(YdbCluster, 'ydb_endpoint', f'{scheme}://seed:1234')
    monkeypatch.setattr(YdbCluster, 'ydb_mon_port', 8765)
    monkeypatch.setattr(YdbCluster, 'get_cluster_nodes', lambda **kwargs: nodes)

    assert TableFiller._discover_static_nodes() == expected


@pytest.mark.parametrize('remote', [False, True])
@pytest.mark.parametrize('input_kind', ['explicit', 'sandbox-multifile', 'sandbox-archive'])
@pytest.mark.parametrize('bundled', [False, True])
def test_query_import_uses_selected_data(monkeypatch, tmp_path, remote, input_kind, bundled):
    from ydb.tests.olap.lib import fill_tables

    suite = tmp_path / 'source'
    schema = suite / 'init' / 'schema.sql'
    schema.parent.mkdir(parents=True)
    schema.write_text('schema')
    bundled_data = suite / 'import' / 'bundled_table'
    if bundled:
        bundled_data.mkdir(parents=True)
    data = tmp_path / 'selected'
    (data / 'selected_table').mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    imports = []

    def check_import(command):
        staged = Path(command[command.index('--suite-path') + 1])
        assert staged != suite
        assert (staged / 'init' / 'schema.sql').read_text() == 'schema'
        assert [entry.name for entry in (staged / 'import').iterdir()] == ['selected_table']
        imports.append(staged)

    def execute(host, command, **kwargs):
        if command[0] == 'ya':
            options = ['--untar'] if input_kind == 'sandbox-archive' else []
            assert command[:-2] == ['ya', 'download', *options, '--output']
            assert command[-1] == 'sbr:42'
            shutil.copytree(data, command[-2])
        elif command[0] == 'ydb':
            check_import(command)
        else:
            subprocess.run(command, check=True)
        return ExecutionResult('', '', False, 0)

    class RemoteImport:
        return_code = 0

        def __init__(self, host, *command):
            check_import(command)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            pass

        def is_running(self):
            return False

    monkeypatch.setattr(fill_tables, 'execute_command', execute)
    monkeypatch.setattr(fill_tables, 'LongRemoteExecution', RemoteImport)
    monkeypatch.setattr(fill_tables, 'copy_file', lambda src, host, dst, **kwargs: shutil.copytree(src, dst))
    worker = fill_tables.Worker('worker', 'grpc://worker:2135', str(tmp_path / 'worker'), 'ydb', (), remote)
    explicit = input_kind == 'explicit'
    dataset = fill_tables.Dataset('query', 'target', suite_path=str(suite), input_path='selected' if explicit else '')
    multifile = input_kind == 'sandbox-multifile'
    resource = None if explicit else {'id': 42, 'file_name': '0' if multifile else 'archive.tar.gz', 'multifile': multifile}
    TableFiller('ydb', attempts=1)._import_part(dataset, worker, 0, 0, 1, resource)

    assert len(imports) == 1
    assert bundled_data.is_dir() == bundled
    assert not (suite / 'import' / 'selected_table').exists()
