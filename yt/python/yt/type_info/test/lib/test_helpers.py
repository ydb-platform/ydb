import pytest

import yt.type_info.typing as typing

from yt.type_info.type_base import (
    is_valid_type, make_primitive_type, validate_type, quote_string, Type
)


def test_valid_type():
    assert is_valid_type(Type)
    assert not is_valid_type(int)
    assert not is_valid_type(3)
    assert is_valid_type(typing.List[typing.Int8])


def test_check_type():
    validate_type(Type)
    validate_type(typing.Int64)
    with pytest.raises(ValueError):
        validate_type(int)
    with pytest.raises(ValueError):
        validate_type(3)
    validate_type(typing.List[typing.Int8])


def test_quoute_string():
    assert quote_string("a'b'c'd") == "'a\\'b\\'c\\'d'"
    assert quote_string("abc") == "'abc'"
    assert quote_string("") == "''"
    assert quote_string("\\bcd\\") == "'\\\\bcd\\\\'"
    assert quote_string("\\b'c'd\\") == "'\\\\b\\'c\\'d\\\\'"
    assert quote_string(u"хэ\\л'ло") == u"'хэ\\\\л\\'ло'"


def test_make_primitive_type():
    MyType = make_primitive_type("MyTypeName")
    assert is_valid_type(MyType)
    assert str(MyType) == "MyTypeName"
    assert MyType.name == "MyTypeName"
    # Check no exception is thrown
    ListOfMyType = typing.List[MyType]
    assert is_valid_type(ListOfMyType)
