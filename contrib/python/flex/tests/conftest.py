import pytest
# XXX: PATCH changed httpbin on pytest-localserver
from pytest_localserver.plugin import httpserver  # noqa


@pytest.fixture()
def msg_assertions():
    from tests.utils import (
        assert_error_message_equal,
        assert_message_in_errors,
        assert_message_not_in_errors,
        assert_path_in_errors,
        assert_path_not_in_errors,
    )

    _dict = {
        'assert_error_message_equal': staticmethod(assert_error_message_equal),
        'assert_message_in_errors': staticmethod(assert_message_in_errors),
        'assert_message_not_in_errors': staticmethod(assert_message_not_in_errors),
        'assert_path_in_errors': staticmethod(assert_path_in_errors),
        'assert_path_not_in_errors': staticmethod(assert_path_not_in_errors),
    }
    return type('AssertionHelpers', (object,), _dict)


@pytest.fixture()
def types():
    from flex.constants import (
        NULL,
        BOOLEAN,
        STRING,
        INTEGER,
        NUMBER,
        ARRAY,
        OBJECT,
    )

    _dict = {
        'NULL': NULL,
        'BOOLEAN': BOOLEAN,
        'STRING': STRING,
        'INTEGER': INTEGER,
        'NUMBER': NUMBER,
        'ARRAY': ARRAY,
        'OBJECT': OBJECT,
    }

    return type('Types', (object,), _dict)


@pytest.fixture()
def MESSAGES():
    from flex.error_messages import MESSAGES
    return MESSAGES
