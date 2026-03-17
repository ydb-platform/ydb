import pytest

from schematics.contrib.enum_type import EnumType
from schematics.exceptions import ConversionError

try:
    from enum import Enum


    class E(Enum):
        A = 1
        B = 'b'


    class F(Enum):
        A = 1
        B = 1

except ImportError:
    Enum = None

pytestmark = pytest.mark.skipif(Enum is None,
                                reason='requires enum')


def test_to_native_by_name():
    field = EnumType(E)
    assert field.to_native("A") == E.A
    assert field.to_native("B") == E.B
    with pytest.raises(ConversionError):
        field.to_native("a")


def test_to_native_by_value():
    field = EnumType(E, use_values=True)
    assert field.to_native(1) == E.A
    assert field.to_native("b") == field.to_native("B")
    with pytest.raises(ConversionError):
        field.to_native(2)


def test_to_native_by_value_duplicate():
    field = EnumType(F, use_values=True)
    assert field.to_native(1) == F.A


def test_passthrough():
    field = EnumType(E, use_values=True)
    assert field.to_native(E.A) == E.A


def test_to_primitive_by_name():
    field = EnumType(E, use_values=False)
    assert field.to_primitive(E.A) == "A"


def test_to_primitive_by_value():
    field = EnumType(E, use_values=True)
    assert field.to_primitive(E.A) == 1
