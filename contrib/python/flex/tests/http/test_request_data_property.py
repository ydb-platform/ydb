import pytest
import json
import sys

import six

from flex.constants import EMPTY
from flex.http import JSONDecodeError

from tests.factories import (
    RequestFactory,
)

def test_null_body_returns_null():
    request = RequestFactory(body=None)
    assert request.data is None


def test_empty_string_body_returns_empty_string():
    request = RequestFactory(body='')
    assert request.data == ''


def test_empty_body_returns_empty():
    request = RequestFactory(body=EMPTY)
    assert request.data is EMPTY


def test_json_content_type_with_json_body():
    request = RequestFactory(
        body=json.dumps({'key': 'value', 'key2': 'value2', 'key[1]': 'subvalue1', 'key[2]': 'subvalue2'}),
        content_type='application/json',
    )
    assert request.data == {'key': 'value', 'key2': 'value2', 'key[1]': 'subvalue1', 'key[2]': 'subvalue2'}


def test_json_content_type_with_json_bytes_body():
    body = json.dumps({'key': 'value', 'key2': 'value2', 'key[1]': 'subvalue1', 'key[2]': 'subvalue2'}).encode('utf-8')
    assert type(body) == bytes
    request = RequestFactory(
        body=body,
        content_type='application/json',
    )
    assert request.data == {'key': 'value', 'key2': 'value2', 'key[1]': 'subvalue1', 'key[2]': 'subvalue2'}


def test_form_content_type_with_body():
    request = RequestFactory(
        body="key=value&key2=value2&arr[1]=subvalue1&arr[2]=subvalue2",
        content_type='application/x-www-form-urlencoded',
    )
    assert request.data == {'key': 'value', 'key2': 'value2', 'arr[1]': 'subvalue1', 'arr[2]': 'subvalue2'}


def test_unsupported_content_type():
    request = RequestFactory(
        body=json.dumps({'key': 'value'}),
        content_type='application/unsupported',
    )
    with pytest.raises(NotImplementedError):
        request.data


@pytest.mark.skipif(sys.version_info[0:2] >= (3, 5), reason='Python35+ has json.JSONDecodeError')
def test_py2_invalid_json():
    body = '{"trailing comma not valid": [1,]}'
    request = RequestFactory(
        body=body,
        content_type='application/json',
    )

    try:
        json.loads(body)
    except ValueError as e:
        expected_message = str(e)

    with pytest.raises(JSONDecodeError) as e:
        request.data
    assert str(e.value) == expected_message


@pytest.mark.skipif(sys.version_info[0:2] < (3, 5), reason='Python2-34 do not have json.JSONDecodeError')
def test_py3_invalid_json():
    body = '{"trailing comma not valid": [1,]}'
    request = RequestFactory(
        body=body,
        content_type='application/json',
    )

    try:
        json.loads(body)
    except JSONDecodeError as e:
        expected_exception = e

    with pytest.raises(JSONDecodeError) as e:
        request.data
    assert e.value.msg == expected_exception.msg


def test_content_type_is_none():
    request = RequestFactory(body='some content', content_type=None)
    with pytest.raises(NotImplementedError):
        request.data
