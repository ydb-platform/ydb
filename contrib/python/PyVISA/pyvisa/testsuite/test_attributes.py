# -*- coding: utf-8 -*-
"""Test attribute descriptors."""

import enum

import pytest

from pyvisa import constants
from pyvisa.attributes import (
    Attribute,
    AttrVI_ATTR_ASRL_BAUD,
    AttrVI_ATTR_INTF_INST_NAME,
    BooleanAttribute,
    CharAttribute,
    EnumAttribute,
    IntAttribute,
    RangeAttribute,
    ValuesAttribute,
)

from . import BaseTestCase


class FakeResource:
    """Fake resource to test attributes."""

    def __init__(self, attr_id, attr_value):
        self.attr_id = attr_id
        self.attr_value = attr_value

    def get_visa_attribute(self, attr_id):
        if attr_id == self.attr_id:
            return self.attr_value
        else:
            raise ValueError()

    def set_visa_attribute(self, attr_id, value):
        if attr_id == self.attr_id:
            self.attr_value = value
        else:
            raise ValueError()


def create_resource_cls(
    attribute_name, attribute_type, read=True, write=True, attrs={}
):
    """Create a new attribute class and a resource using it."""
    attrs.update({"attribute_id": attribute_name, "read": read, "write": write})
    attr_cls = type("CA", (attribute_type,), attrs)

    return type("FakeR", (FakeResource,), {"attr": attr_cls()})


class TestAttributeClasses(BaseTestCase):
    """Test the descriptors used to handle VISA attributes."""

    def test_in_resource_method(self):
        """Test the in_resource class method."""
        assert AttrVI_ATTR_INTF_INST_NAME.in_resource(object())
        assert AttrVI_ATTR_ASRL_BAUD.in_resource(
            (constants.InterfaceType.asrl, "INSTR")
        )
        assert not AttrVI_ATTR_ASRL_BAUD.in_resource(object())

    def test_Attribute(self):
        """Test the base class Attribute."""
        rc = create_resource_cls("attr_id", Attribute)
        r = rc("attr_id", 1)
        assert r.attr == 1
        r.attr = 2
        assert r.attr == 2

        # Check we do pass the write ID
        r.attr_id = "dummy"
        with pytest.raises(ValueError):
            r.attr
        with pytest.raises(ValueError):
            r.attr = 2

        # Un-readable attribute
        rc = create_resource_cls("attr_id", Attribute, read=False)
        r = rc("attr_id", 1)
        with pytest.raises(AttributeError):
            r.attr

        # Un-writable attribute
        rc = create_resource_cls("attr_id", Attribute, write=False)
        r = rc("attr_id", 1)
        with pytest.raises(AttributeError):
            r.attr = 1

    def test_BooleanAttribute(self):
        """Test BooleanAttribute."""
        rc = create_resource_cls("attr_id", BooleanAttribute)
        r = rc("attr_id", constants.VI_TRUE)
        assert r.attr is True
        r.attr = False
        assert r.attr is False
        assert r.attr_value == constants.VI_FALSE

    def test_CharAttribute(self):
        """Test CharAttribute."""
        rc = create_resource_cls("attr_id", CharAttribute)
        r = rc("attr_id", ord("\n"))
        assert r.attr == "\n"
        r.attr = "\r"
        assert r.attr == "\r"
        assert r.attr_value == 13

    def test_EnumAttribute(self):
        """Test EnumAttribute"""

        @enum.unique
        class E(enum.IntEnum):
            a = 1
            b = 2

        rc = create_resource_cls("attr_id", EnumAttribute, attrs={"enum_type": E})
        r = rc("attr_id", 1)
        assert r.attr == E.a
        r.attr = E.b
        assert r.attr == E.b
        assert r.attr_value == 2

        with pytest.raises(ValueError):
            r.attr = 3

        with pytest.raises(ValueError):
            r.attr = ""

    def test_IntAttribute(self):
        """Test IntAttribute."""
        rc = create_resource_cls("attr_id", IntAttribute)
        r = rc("attr_id", "1")
        assert r.attr == 1

    def test_RangeAttribute(self):
        """Test RangeAttribute"""
        rc = create_resource_cls(
            "attr_id", RangeAttribute, attrs={"min_value": 0, "max_value": 2}
        )
        r = rc("attr_id", 1)
        r.attr = 0
        assert r.attr_value == 0
        r.attr = 2
        assert r.attr_value == 2
        r.attr = 1
        assert r.attr_value == 1

        with pytest.raises(ValueError) as cm:
            r.attr = -1

        assert "invalid value" in str(cm.exconly())
        assert " or " not in str(cm.exconly())

        with pytest.raises(ValueError) as cm:
            r.attr = 3

        assert "invalid value" in str(cm.exconly())
        assert " or " not in str(cm.exconly())

        rc = create_resource_cls(
            "attr_id",
            RangeAttribute,
            attrs={"min_value": 0, "max_value": 2, "values": [10]},
        )
        r = rc("attr_id", 1)
        r.attr = 10
        assert r.attr_value == 10

        with pytest.raises(ValueError) as cm:
            r.attr = 3

        assert "invalid value" in str(cm.exconly())
        assert " or " in str(cm.exconly())

    def test_ValuesAttribute(self):
        """Test ValuesAttribute"""
        rc = create_resource_cls("attr_id", ValuesAttribute, attrs={"values": [10, 20]})
        r = rc("attr_id", 1)
        r.attr = 10
        assert r.attr_value == 10

        with pytest.raises(ValueError) as cm:
            r.attr = 3
        assert "invalid value" in str(cm.exconly())
