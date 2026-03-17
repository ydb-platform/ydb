# -*- coding: utf-8 -*-

import pytest
import json

from schematics.exceptions import *


def test_error_from_string():

    e = ConversionError('hello')
    assert e.messages == ['hello']

    e = ValidationError('hello', 'world', '!')
    assert e.messages == ['hello', 'world', '!']
    assert len(e) == 3


def _assert(e):
    assert e.messages == ['hello'] and e.messages[0].info == 99


def test_error_from_args():
    _assert(ValidationError('hello', info=99))


def test_error_from_tuple():
    _assert(ValidationError(('hello', 99)))


def test_error_from_message():
    _assert(ValidationError(ErrorMessage('hello', info=99)))


def test_error_from_error():
    _assert(ValidationError(ValidationError(('hello', 99))))


def test_error_from_mixed_args():

    e = ValidationError(
            ('hello', 99),
            'world',
            ErrorMessage('from_msg', info=0),
            ValidationError('from_err', info=1))

    assert e == e.messages == ['hello', 'world', 'from_msg', 'from_err']
    assert [msg.info for msg in e] == [99, None, 0, 1]


def test_error_from_mixed_list():

    e = ConversionError([
            ('hello', 99),
            'world',
            ErrorMessage('from_msg', info=0),
            ConversionError('from_err', info=1)])

    assert e.messages == ['hello', 'world', 'from_msg', 'from_err']
    assert [msg.info for msg in e.messages] == [99, None, 0, 1]


def test_error_str():

    assert str(ValidationError('foo')) == '["foo"]'

    e = ValidationError(
            ('foo', None),
            ('bar', 98),
            ('baz', [1, 2, 3]))

    assert str(e) == '["foo", "bar: 98", "baz: [1, 2, 3]"]'


def test_error_list_conversion():
    err = ValidationError("A", "B", "C")
    assert list(err) == err.messages


def test_error_eq():
    assert ValidationError("A") == ValidationError("A") == ["A"]
    assert ValidationError("A") != ConversionError("A")
    assert ValidationError("A", "B") == ValidationError("A", "B") == ["A", "B"]
    assert ValidationError("A") != ValidationError("A", "B")


def test_error_message_object():

    assert ErrorMessage('foo') == 'foo'
    assert ErrorMessage('foo') != 'bar'
    assert ErrorMessage('foo', 1) == ErrorMessage('foo', 1)
    assert ErrorMessage('foo', 1) != ErrorMessage('foo', 2)


@pytest.mark.parametrize("error", [
    ErrorMessage('foo', info='bar'),
    BaseError([ErrorMessage('foo', info='bar')]),
    BaseError({"foo": "bar"}),
    ErrorMessage(u'é'),
    ValidationError(u'é')
])
def test_exception_repr(error):
    assert error == eval(repr(error))


def test_error_failures():

    with pytest.raises(NotImplementedError):
        FieldError()

    with pytest.raises(TypeError):
        ValidationError()

    with pytest.raises(TypeError):
        ValidationError('hello', 99)

    with pytest.raises(TypeError):
        ConversionError(ValidationError('hello'))

    with pytest.raises(TypeError):
        CompoundError(['hello'])


def test_to_primitive():
    error = BaseError({
        'a': [ErrorMessage('a1'), ErrorMessage('a2')],
        'b': {
            'd': ErrorMessage('d_val'),
            'e': ErrorMessage('e_val'),
        },
        'c': ErrorMessage('this is an error')
    })
    assert error.to_primitive() == {
        'a': ['a1', 'a2'],
        'b': {
            'd': 'd_val',
            'e': 'e_val'
        },
        'c': 'this is an error'
    }


def test_to_primitive_list():
    error = BaseError([ErrorMessage('a1'), ErrorMessage('a2')])
    assert error.to_primitive() == ['a1', 'a2']


def test_autopopulate_message_on_none():
    errors = {
        'a': [ErrorMessage('a1'), ErrorMessage('a2')],
        'b': {
            'd': ErrorMessage('d_val'),
            'e': ErrorMessage('e_val'),
        },
        'c': ErrorMessage('this is an error')
    }
    e = BaseError(errors)
    assert json.loads(str(e)) == BaseError._to_primitive(errors)


@pytest.mark.parametrize("e", [
    BaseError(["a", "b"]),
    ConversionError(ErrorMessage("foo"), ErrorMessage("bar")),
    CompoundError({"a": ValidationError(ErrorMessage("foo"))})
])
def test_exceptions_is_hashable(e):
    """exceptions must be hashable, as the logging module expects this
    for log.exception()
    """
    hash(e)


@pytest.mark.parametrize("inp,out", [
    (ConversionError(ErrorMessage("foo")), ["foo"])
])
def test_clean_str_representation(inp, out):
    """
    the string representation should be human-readable.  json's format
    provides a legible format for complex data types.
    """
    assert str(inp) == json.dumps(out)
