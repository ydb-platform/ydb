"""Tests for the ijson.basic_parse method"""

import itertools
import threading
from decimal import Decimal

import pytest
from ijson import common

from .test_base import ARRAY_JSON, ARRAY_JSON_EVENTS, INCOMPLETE_JSONS, INCOMPLETE_JSON_TOKENS, INVALID_JSONS, JSON, JSON_EVENTS, SCALAR_JSON, SURROGATE_PAIRS_JSON, STRINGS_JSON


def _raises_json_error(adaptor, json, **kwargs):
    with pytest.raises(common.JSONError):
        adaptor.basic_parse(json, **kwargs)


def _raises_incomplete_json_error(adaptor, json):
    with pytest.raises(common.IncompleteJSONError):
        adaptor.basic_parse(json)


def test_basic_parse(adaptor):
    assert JSON_EVENTS == adaptor.basic_parse(JSON)


def test_basic_parse_threaded(adaptor):
    thread = threading.Thread(target=test_basic_parse, args=(adaptor,))
    thread.start()
    thread.join()


def test_basic_parse_array(adaptor):
    assert ARRAY_JSON_EVENTS == adaptor.basic_parse(ARRAY_JSON)


def test_basic_parse_array_threaded(adaptor):
    thread = threading.Thread(target=test_basic_parse_array, args=(adaptor,))
    thread.start()
    thread.join()


def test_scalar(adaptor):
    assert [('number', 0)] == adaptor.basic_parse(SCALAR_JSON)


def test_strings(adaptor):
    events = adaptor.basic_parse(STRINGS_JSON)
    strings = [value for event, value in events if event == 'string']
    assert ['', '"', '\\', '\\\\', '\b\f\n\r\t'] == strings
    assert ('map_key', 'special\t') in events


def test_surrogate_pairs(adaptor):
    event = adaptor.basic_parse(SURROGATE_PAIRS_JSON)[0]
    parsed_string = event[1]
    assert '💩' == parsed_string


def _get_numbers(adaptor, json, use_float):
    events = adaptor.basic_parse(json, use_float=use_float)
    return [value for event, value in events if event == 'number']


@pytest.mark.parametrize(
    "json, expected_float_type, expected_numbers, use_float",
    (
        (b'[1, 1.0, 1E2]', Decimal, [1, Decimal("1.0"), Decimal("1e2")], False),
        (b'[1, 1.0, 1E2]', float, [1, 1., 100.], True),
        (b'1e400', Decimal, [Decimal('1e400')], False),
        (b'1e-400', Decimal, [Decimal('1e-400')], False),
        (b'1e-400', float, [0], True),
    )
)
def test_numbers(adaptor, json, expected_float_type, expected_numbers, use_float):
    """Check that numbers are correctly parsed"""
    numbers = _get_numbers(adaptor, json, use_float=use_float)
    float_types = set(type(number) for number in numbers)
    float_types -= {int}
    assert 1 == len(float_types)
    assert expected_float_type == next(iter(float_types))
    assert expected_numbers == numbers


def test_32bit_ints(adaptor):
    """Test for 64-bit integers support when using use_float=true"""
    past32bits = 2 ** 32 + 1
    past32bits_as_json = ('%d' % past32bits).encode('utf8')
    if adaptor.backend.capabilities.int64:
        parsed_number = _get_numbers(adaptor, past32bits_as_json, use_float=True)[0]
        assert past32bits == parsed_number
    else:
        _raises_json_error(adaptor, past32bits_as_json, use_float=True)


def test_max_double(adaptor):
    """Check that numbers bigger than MAX_DOUBLE (usually ~1e308) cannot be represented"""
    _raises_json_error(adaptor, b'1e400', use_float=True)


@pytest.mark.parametrize(
    "json", [
        sign + prefix + suffix
        for sign, prefix, suffix in itertools.product(
            (b'', b'-'),
            (b'00', b'01', b'001'),
            (b'', b'.0', b'e0', b'E0')
        )
    ]
)
def test_invalid_leading_zeros(adaptor, json):
    """Check leading zeros are invalid"""
    if not adaptor.backend.capabilities.invalid_leading_zeros_detection:
        return
    _raises_json_error(adaptor, json)


@pytest.mark.parametrize("json", (b'1e', b'0.1e', b'0E'))
def test_incomplete_exponents(adaptor, json):
    """incomplete exponents are invalid JSON"""
    _raises_json_error(adaptor, json)


@pytest.mark.parametrize("json", (b'1.', b'.1'))
def test_incomplete_fractions(adaptor, json):
    """incomplete fractions are invalid JSON"""
    _raises_json_error(adaptor, json)


def test_incomplete(adaptor):
    for json in INCOMPLETE_JSONS:
        _raises_incomplete_json_error(adaptor, json)


def test_incomplete_tokens(adaptor):
    if not adaptor.backend.capabilities.incomplete_json_tokens_detection:
        return
    for json in INCOMPLETE_JSON_TOKENS:
        _raises_incomplete_json_error(adaptor, json)


def test_invalid(adaptor):
    for json in INVALID_JSONS:
        if not adaptor.backend.capabilities.incomplete_json_tokens_detection and json == INVALID_JSON_WITH_DANGLING_JUNK:
            continue
        _raises_json_error(adaptor, json)


def test_comments(adaptor):
    json = b'{"a": 2 /* a comment */}'
    if adaptor.backend.capabilities.c_comments:
        events = adaptor.basic_parse(json, allow_comments=True)
        assert events is not None
    else:
        with pytest.raises(ValueError):
            adaptor.basic_parse(json, allow_comments=True)


def test_multiple_values_raises_if_not_supported(adaptor):
    """Test that setting multiple_values raises if not supported"""
    if not adaptor.backend.capabilities.multiple_values:
        with pytest.raises(ValueError):
            adaptor.basic_parse("", multiple_values=True)


def test_multiple_values(adaptor):
    """Test that multiple_values are supported"""
    multiple_json = JSON + JSON + JSON
    with pytest.raises(common.JSONError):
        adaptor.basic_parse(multiple_json)
    with pytest.raises(common.JSONError):
        adaptor.basic_parse(multiple_json, multiple_values=False)
    result = adaptor.basic_parse(multiple_json, multiple_values=True)
    assert JSON_EVENTS + JSON_EVENTS + JSON_EVENTS == result
