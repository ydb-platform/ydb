import itertools
import pytest

from flex.loading.schema import (
    swagger_schema_validator,
)
from flex.loading.schema.host import decompose_hostname
from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES

from tests.utils import (
    assert_message_in_errors,
    assert_path_not_in_errors,
    assert_path_in_errors,
)
from tests.factories import (
    RawSchemaFactory,
)


@pytest.mark.parametrize(
    'scheme,hostname,port,path',
    itertools.product(
        ('http', ''),
        ('127.0.0.1', 'example.com', 'www.example.com'),
        ('8000', ''),
        ('api', ''),
    ),
)
def test_hostname_decomposition(scheme, hostname, port, path):
    """
    Ensure that the hostname decomposition tool works as expected.
    """
    value = hostname
    if scheme:
        value = "{0}://{1}".format(scheme, value)

    if port:
        value = "{0}:{1}".format(value, port)

    if path:
        value = "{0}/{1}".format(value, path)

    assert decompose_hostname(value) == (scheme, hostname, port, path)


def test_host_is_not_required():
    """
    Test that the info field is required for overall schema validation.
    """
    raw_schema = RawSchemaFactory()
    raw_schema.pop('host', None)

    assert 'host' not in raw_schema

    try:
        swagger_schema_validator(raw_schema)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'host',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    (
        '127.0.0.1',
        '127.0.0.1:8000',
        'example.com',
        'example.com:8000',
        'www.example.com',
        'www.example.com:8000',
    ),
)
def test_valid_host_values(value):
    raw_schema = RawSchemaFactory(host=value)

    try:
        swagger_schema_validator(raw_schema)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'host',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    (
        '127.0.0.1/api',
        '127.0.0.1:8000/api',
        'example.com/api',
        'example.com:8000/api',
        'www.example.com/api',
        'www.example.com:8000/api',
    ),
)
def test_invalid_host_value_with_path(value):
    raw_schema = RawSchemaFactory(host=value)

    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(raw_schema)

    assert_message_in_errors(
        MESSAGES['host']['may_not_include_path'],
        err.value.detail,
        'host.path',
    )


@pytest.mark.parametrize(
    'value',
    (
        'http://127.0.0.1/api',
        'http://127.0.0.1:8000/api',
        'http://example.com/api',
        'http://example.com:8000/api',
        'http://www.example.com/api',
        'http://www.example.com:8000/api',
    ),
)
def test_invalid_host_value_with_scheme(value):
    raw_schema = RawSchemaFactory(host=value)

    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(raw_schema)

    assert_message_in_errors(
        MESSAGES['host']['may_not_include_scheme'],
        err.value.detail,
        'host.scheme',
    )
