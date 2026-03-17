from datetime import datetime
from decimal import Decimal

import isodate
import pytest
from lxml import etree

from zeep.xsd import types


def test_base_type():
    # Basically just for coverage... ;-)
    base = types.Type()
    with pytest.raises(NotImplementedError):
        base.accept("x")

    with pytest.raises(NotImplementedError):
        base.parse_xmlelement(None)

    with pytest.raises(NotImplementedError):
        base.parsexml(None)

    with pytest.raises(NotImplementedError):
        base.render(None, None)

    with pytest.raises(NotImplementedError):
        base.resolve()

    base.signature() == ""


def test_simpletype_eq():
    type_1 = types.AnySimpleType()
    type_2 = types.AnySimpleType()

    assert type_1 == type_2


def test_simpletype_parse():
    node = etree.Element("foobar")
    item = types.AnySimpleType()

    assert item.parse_xmlelement(node) is None


def test_simpletype_pythonvalue_string():
    item = types.AnySimpleType()
    value = "foobar"

    assert item.pythonvalue(value) == value


def test_simpletype_pythonvalue_bool():
    item = types.AnySimpleType()
    value = False

    assert item.pythonvalue(value) == value


def test_simpletype_pythonvalue_decimal():
    item = types.AnySimpleType()
    value = Decimal("3.14")

    assert item.pythonvalue(value) == value


def test_simpletype_pythonvalue_float():
    item = types.AnySimpleType()
    value = 3.14

    assert item.pythonvalue(value) == value


def test_simpletype_pythonvalue_duration():
    item = types.AnySimpleType()
    value = isodate.parse_duration("P1Y2M3DT4H5M6S")

    assert item.pythonvalue(value) == value


def test_simpletype_pythonvalue_datetime():
    item = types.AnySimpleType()
    value = datetime.now()

    assert item.pythonvalue(value) == value


def test_simpletype_pythonvalue_time():
    item = types.AnySimpleType()
    value = datetime.now().time()

    assert item.pythonvalue(value) == value


def test_simpletype_pythonvalue_date():
    item = types.AnySimpleType()
    value = datetime.now().date()

    assert item.pythonvalue(value) == value


def test_simpletype_call_wrong_arg_count():
    item = types.AnySimpleType()

    with pytest.raises(TypeError):
        item("foo", "bar")


def test_simpletype_call_wrong_kwarg():
    item = types.AnySimpleType()

    with pytest.raises(TypeError):
        item(uhhh="x")


def test_simpletype_str():
    item = types.AnySimpleType()
    item.name = "foobar"
    assert str(item) == "AnySimpleType(value)"


def test_complextype_parse_xmlelement_no_childs():
    xmlelement = etree.Element("foobar")
    item = types.ComplexType()
    assert item.parse_xmlelement(xmlelement, None) is None
