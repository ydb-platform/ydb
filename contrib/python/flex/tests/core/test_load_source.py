from __future__ import unicode_literals

import tempfile

import six

import json
import yaml

from flex._compat import Mapping
from flex.core import load_source


def test_native_mapping_is_passthrough():
    source = {'foo': 'bar'}
    result = load_source(source)

    assert result == source
    assert result is not source


def test_json_string():
    native = {'foo': 'bar'}
    source = json.dumps(native)
    result = load_source(source)

    assert result == native


def test_yaml_string():
    native = {b'foo': b'bar'}
    source = yaml.dump(native)
    result = load_source(source)

    assert result == native


def test_json_file_object():
    native = {'foo': 'bar'}
    source = json.dumps(native)

    tmp_file = tempfile.NamedTemporaryFile(mode='w')
    tmp_file.write(source)
    tmp_file.file.seek(0)

    with open(tmp_file.name) as json_file:
        result = load_source(json_file)

    assert result == native


def test_json_file_path():
    native = {'foo': 'bar'}
    source = json.dumps(native)

    tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json')
    tmp_file.write(source)
    tmp_file.flush()

    result = load_source(tmp_file.name)

    assert result == native


def test_yaml_file_object():
    native = {b'foo': b'bar'}
    source = yaml.dump(native)

    tmp_file = tempfile.NamedTemporaryFile(mode='w')
    tmp_file.write(source)
    tmp_file.flush()

    with open(tmp_file.name) as yaml_file:
        result = load_source(yaml_file)

    assert result == native


def test_yaml_file_path():
    native = {b'foo': b'bar'}
    source = yaml.dump(native)

    tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml')
    tmp_file.write(source)
    tmp_file.flush()

    result = load_source(tmp_file.name)

    assert result == native


def test_url(httpserver):
    native = {
        'origin': '127.0.0.1',
        #'headers': {
        #    'Content-Length': '',
        #    'Accept-Encoding': 'gzip, deflate',
        #    'Host': '127.0.0.1:54634',
        #    'Accept': '*/*',
        #    'User-Agent': 'python-requests/2.4.3 CPython/2.7.8 Darwin/14.0.0',
        #    'Connection': 'keep-alive',
        #},
        'args': {},
        #'url': 'http://127.0.0.1:54634/get',
    }
    httpserver.serve_content(
        json.dumps(native),
        code=200,
        headers={'content-type': 'application/json'},
    )
    source = httpserver.url + '/get'
    result = load_source(source)
    assert isinstance(result, Mapping)
    # XXX: because changed httpbin on httpserver
    # result.pop('headers')
    # result.pop('url')
    assert result == native
