import io
from unittest import mock

import pytest

from ydb.apps.dstool.lib import common


@pytest.fixture
def token_home(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.delenv('YDB_TOKEN', raising=False)
    monkeypatch.delenv('IAM_TOKEN', raising=False)
    home = tmp_path / '.ydb'
    home.mkdir()
    return home


@pytest.mark.parametrize('filename, token_type', [('token', 'OAuth'), ('iam_token', 'Bearer')])
@pytest.mark.parametrize(
    'value, expected_type, expected_token',
    [
        ('secret\r\nignored', None, 'secret'),
        ('Bearer explicit\n', 'Bearer', 'explicit'),
        ('root@builtin\n', None, 'root@builtin'),
    ],
)
def test_default_token_file(token_home, filename, token_type, value, expected_type, expected_token):
    (token_home / filename).write_bytes(value.encode())
    params = common.ConnectionParams()
    params.parse_token(None)
    assert params.token == expected_token
    assert params.token_type == (None if expected_token.endswith('@builtin') else expected_type or token_type)


def test_missing_default_token_files(token_home):
    params = common.ConnectionParams()
    params.parse_token(None)
    assert params.token is None


@pytest.mark.parametrize('first_source', range(6))
def test_token_source_priority(token_home, monkeypatch, first_source):
    sources = ['cli-oauth', 'cli-iam', 'env-oauth', 'env-iam', 'home-oauth', 'home-iam']
    if first_source <= 4:
        (token_home / 'token').write_text(sources[4])
    (token_home / 'iam_token').write_text(sources[5])
    if first_source <= 2:
        monkeypatch.setenv('YDB_TOKEN', sources[2])
    if first_source <= 3:
        monkeypatch.setenv('IAM_TOKEN', sources[3])
    token_file = io.StringIO(sources[0]) if first_source == 0 else None
    iam_token_file = io.StringIO(sources[1]) if first_source <= 1 else None
    params = common.ConnectionParams()
    try:
        params.parse_token(token_file, iam_token_file)
        assert params.token == sources[first_source]
        assert params.token_type == ('OAuth' if first_source % 2 == 0 else 'Bearer')
    finally:
        for stream in (token_file, iam_token_file):
            if stream is not None:
                stream.close()


def test_empty_default_token_preserves_priority(token_home):
    (token_home / 'token').write_text('')
    (token_home / 'iam_token').write_text('fallback')
    params = common.ConnectionParams()
    params.parse_token(None)
    assert (params.token_type, params.token) == ('OAuth', '')


@pytest.mark.parametrize('error', [None, OSError('read failed'), AttributeError('programming error')])
def test_token_file_closed_and_errors_handled(error):
    stream = io.StringIO('secret\n')
    params = common.ConnectionParams()
    with mock.patch.object(common, 'open', return_value=stream, create=True):
        with mock.patch.object(params, 'read_token_from_file', side_effect=error, return_value=('OAuth', 'secret')):
            if isinstance(error, AttributeError):
                with pytest.raises(AttributeError, match='programming error'):
                    params.read_token_file('token', 'OAuth')
            else:
                assert params.read_token_file('token', 'OAuth') == ('OAuth', None if error else 'secret')
    assert stream.closed


def test_missing_token_path():
    assert common.ConnectionParams().read_token_file(None, 'Bearer') == ('Bearer', None)
