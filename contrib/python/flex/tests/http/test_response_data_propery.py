import pytest
import json
import sys

import six

from flex.http import JSONDecodeError

from tests.factories import (
    ResponseFactory,
)


@pytest.mark.skipif(sys.version_info[0:2] >= (3, 5), reason='Python35+ has json.JSONDecodeError')
def test_py2_invalid_json():
    body = '{"trailing comma not valid": [1,]}'
    response = ResponseFactory(
        content=body,
        content_type='application/json',
    )

    try:
        json.loads(body)
    except ValueError as e:
        expected_message = str(e)

    with pytest.raises(JSONDecodeError) as e:
        response.data
    assert str(e.value) == expected_message


@pytest.mark.skipif(sys.version_info[0:2] < (3, 5), reason='Python2-34 do not have json.JSONDecodeError')
def test_py3_invalid_json():
    body = '{"trailing comma not valid": [1,]}'
    response = ResponseFactory(
        content=body,
        content_type='application/json',
    )

    try:
        json.loads(body)
    except JSONDecodeError as e:
        expected_exception = e

    with pytest.raises(JSONDecodeError) as e:
        response.data
    assert e.value.msg == expected_exception.msg


def test_content_type_is_none():
    request = ResponseFactory(content='some content', content_type=None)
    with pytest.raises(NotImplementedError):
        request.data
