import argparse
import io
from unittest import mock

import pytest

from ydb.apps.dstool.lib import common


@pytest.fixture
def connection(tmp_path, monkeypatch):
    for name in ('YDB_TOKEN', 'IAM_TOKEN', 'YDB_USER', 'YDB_PASSWORD'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv('HOME', str(tmp_path))
    params = common.ConnectionParams()
    monkeypatch.setattr(common, 'connection_params', params)
    return params


def apply_args(params, *options):
    parser = argparse.ArgumentParser()
    params.add_host_access_options(parser)
    params.apply_args(parser.parse_args(list(options)))


def login_response(token='login-token', ready=True, status=common.StatusIds.SUCCESS):
    response = common.ydb_auth.LoginResponse()
    response.operation.ready = ready
    response.operation.status = status
    response.operation.result.Pack(common.ydb_auth.LoginResult(token=token))
    return response


@pytest.mark.parametrize('source', ['token-file', 'iam-token-file', 'YDB_TOKEN', 'IAM_TOKEN', 'token', 'iam_token'])
def test_token_auth_ignores_environment_password(connection, tmp_path, monkeypatch, source):
    monkeypatch.setenv('YDB_PASSWORD', 'unrelated-password')
    options = []
    if source.endswith('-file'):
        path = tmp_path / 'cli-token'
        path.write_text('existing-token')
        options = ['--' + source, str(path)]
    elif source.isupper():
        monkeypatch.setenv(source, 'existing-token')
    else:
        home = tmp_path / '.ydb'
        home.mkdir()
        (home / source).write_text('existing-token')
    with mock.patch.object(common, 'invoke_grpc') as invoke:
        apply_args(connection, '-e', 'grpcs://host:2135', *options)
    invoke.assert_not_called()
    assert connection.token == 'existing-token'


def test_password_environment_without_user_is_ignored(connection, monkeypatch):
    monkeypatch.setenv('YDB_PASSWORD', 'unrelated-password')
    with mock.patch.object(common, 'invoke_grpc') as invoke:
        apply_args(connection, '-e', 'grpc://host:2135')
    invoke.assert_not_called()
    assert connection.token is None


@pytest.mark.parametrize('source', ['file', 'empty', 'environment', 'prompt'])
def test_password_sources_and_http_token(connection, tmp_path, monkeypatch, source):
    options = ['--user', 'alice']
    password = 'пароль with spaces'
    monkeypatch.setenv('YDB_TOKEN', 'ignored-token')
    monkeypatch.setenv('YDB_PASSWORD', 'environment-password')
    if source == 'file':
        path = tmp_path / 'password'
        path.write_bytes((password + '\r\nignored').encode())
        options += ['--password-file', str(path)]
    elif source == 'empty':
        options += ['--no-password']
        password = ''
    elif source == 'environment':
        password = 'environment-password'
    else:
        monkeypatch.delenv('YDB_PASSWORD')
    with mock.patch.object(common.getpass, 'getpass', return_value=password) as prompt:
        with mock.patch.object(common, 'invoke_grpc', return_value=login_response()) as invoke:
            apply_args(connection, '-e', 'grpcs://host:2135', *options)
    request = invoke.call_args.args[1]
    assert (request.user, request.password) == ('alice', password)
    assert prompt.call_count == (1 if source == 'prompt' else 0)
    assert (connection.token_type, connection.token) == ('Login', 'login-token')
    with mock.patch.object(common.urllib.request, 'urlopen') as urlopen:
        urlopen.return_value.__enter__.return_value.read.return_value = b'{}'
        common.fetch('viewer/json/sysinfo', endpoint=common.EndpointInfo('https', 'host', 2135, 8765), fmt='raw')
    assert urlopen.call_args.args[0].get_header('Authorization') == 'Login login-token'


def test_environment_user(connection, monkeypatch):
    monkeypatch.setenv('YDB_USER', 'alice@ldap')
    monkeypatch.setenv('YDB_PASSWORD', '')
    with mock.patch.object(common, 'invoke_grpc', return_value=login_response()) as invoke:
        apply_args(connection, '-e', 'grpcs://host:2135')
    assert invoke.call_args.args[1].user == 'alice@ldap'
    assert invoke.call_args.args[1].password == ''


@pytest.mark.parametrize('option', ['--password-file', '--no-password'])
def test_explicit_password_requires_user(connection, tmp_path, option):
    path = tmp_path / 'password'
    path.write_text('password')
    options = [option, str(path)] if option == '--password-file' else [option]
    with pytest.raises(common.InvalidParameterError, match='User name is required'):
        apply_args(connection, '-e', 'grpc://host:2135', *options)


def test_password_file_is_closed(connection):
    stream = io.StringIO('password\n')
    connection.parse_login('alice', stream, False)
    assert stream.closed


@pytest.mark.parametrize(
    'endpoints, expected_protocol, expected_host, expected_port',
    [
        (['https://secure:8765'], 'grpcs', 'secure', 2135),
        (['grpcs://secure:2136', 'http://plain:8765'], 'grpcs', 'secure', 2136),
        (['https://secure:8765', 'grpc://plain:2135'], 'grpcs', 'secure', 2135),
        (['grpcs://secure:2136', 'grpc://plain:2135'], 'grpcs', 'secure', 2136),
        (['http://plain:8765'], 'grpc', 'plain', 2135),
        (['grpc://plain:2136'], 'grpc', 'plain', 2136),
    ],
)
def test_login_endpoint_selection(connection, endpoints, expected_protocol, expected_host, expected_port):
    options = [arg for endpoint in endpoints for arg in ('-e', endpoint)]
    with mock.patch.object(common, 'invoke_grpc', return_value=login_response()) as invoke:
        apply_args(connection, *options, '--user', 'alice', '--no-password')
    selected = invoke.call_args.kwargs['endpoints']
    assert [(endpoint.protocol, endpoint.host, endpoint.grpc_port) for endpoint in selected] == [
        (expected_protocol, expected_host, expected_port)
    ]


@pytest.mark.parametrize('fail', [False, True])
def test_login_debug_redaction_and_tls(connection, capsys, fail):
    response = login_response(token='synthetic-secret-token')
    stub = mock.Mock()
    stub.Login.return_value = response
    if fail:
        stub.Login.side_effect = ValueError('synthetic-password synthetic-secret-token')
    with mock.patch.object(common.auth_grpc_server, 'AuthServiceStub', return_value=stub):
        with mock.patch.object(common.grpc, 'secure_channel') as secure:
            with mock.patch.object(common.grpc, 'insecure_channel') as insecure:
                with mock.patch.object(common.getpass, 'getpass', return_value='synthetic-password'):
                    options = ('-e', 'https://secure:8765', '-e', 'http://plain:8765', '--user', 'alice', '--debug')
                    if fail:
                        with pytest.raises(common.ConnectionError):
                            apply_args(connection, *options)
                    else:
                        apply_args(connection, *options)
                assert secure.call_count >= 1
                assert all(call.args[0] == 'secure:2135' for call in secure.call_args_list)
                insecure.assert_not_called()
    stderr = capsys.readouterr().err
    assert 'issuing Login' in stderr
    assert 'synthetic-password' not in stderr
    assert 'synthetic-secret-token' not in stderr


@pytest.mark.parametrize('failure', ['not-ready', 'status', 'wrong-result', 'empty-token'])
def test_invalid_login_response(connection, failure):
    response = login_response()
    if failure == 'not-ready':
        response.operation.ready = False
    elif failure == 'status':
        response.operation.status = common.StatusIds.UNAUTHORIZED
        response.operation.issues.add(message='Access denied')
    elif failure == 'wrong-result':
        response.operation.result.Clear()
    else:
        response = login_response(token='')
    with mock.patch.object(common, 'invoke_grpc', return_value=response):
        with pytest.raises(common.QueryError, match='Login'):
            apply_args(connection, '-e', 'grpcs://host:2135', '--user', 'alice', '--no-password')
    assert connection.token is None
