# -*- coding: utf-8 -*-
"""Common test case for all resources."""

import gc
import logging
import re
from typing import Union

import pytest

from pyvisa import InvalidSession, ResourceManager
from pyvisa.constants import InterfaceType, ResourceAttribute, StatusCode, Timeouts
from pyvisa.resources.resource import Resource
from pyvisa.rname import ResourceName

from . import RESOURCE_ADDRESSES


class ResourceTestCase:
    """Base test case for all resources."""

    #: Type of resource being tested in this test case.
    #: See RESOURCE_ADDRESSES in the __init__.py file of this package for
    #: acceptable values
    RESOURCE_TYPE = ""

    #: Minimal timeout value accepted by the resource. When setting the timeout
    #: to Timeouts.immediate, Visa (Keysight at least) may actually use a
    #: different value depending on the values supported by the resource.
    MINIMAL_TIMEOUT: Union[int, Timeouts] = Timeouts.immediate

    def setup_method(self):
        """Create a resource using the address matching the type."""
        name = RESOURCE_ADDRESSES[self.RESOURCE_TYPE]
        self.rname = ResourceName.from_string(name)
        self.rm = ResourceManager()
        self.instr = self.rm.open_resource(name)
        self.instr.clear()

    def teardown_method(self):
        """Close the resource at the end of the test."""
        if self.instr:
            self.instr.close()
        if self.rm:
            self.rm.close()

    def test_lifecycle(self):
        """Test the lifecyle of a resource and the use as a context manager."""
        assert self.instr.session is not None
        assert self.instr.visalib is not None
        assert self.instr.last_status == StatusCode.success

        self.instr.close()

        with pytest.raises(InvalidSession):
            self.instr.session

        with self.rm.open_resource(str(self.rname), read_termination="\0") as instr:
            assert len(self.rm.list_opened_resources()) == 1
            assert instr.read_termination == "\0"
        assert len(self.rm.list_opened_resources()) == 0

    def test_close_on_del(self, caplog):
        """Test the lifecyle of a resource and the use as a context manager."""
        with caplog.at_level(logging.DEBUG):
            self.instr = None
            gc.collect()

        assert "- closing" in caplog.records[0].message
        assert "- is closed", caplog.output[-1].message

    def test_alias_bypassing(self):
        """Test that a resource that cannot normalize an alias keep the alias."""
        instr = Resource(self.rm, "visa_alias")
        assert re.match(r".* at %s" % "visa_alias", str(instr))

    def test_str(self):
        """Test the string representation of a resource."""
        assert re.match(r".* at %s" % str(self.rname), str(self.instr))
        self.instr.close()
        assert re.match(r".* at %s" % str(self.rname), str(self.instr))

    def test_repr(self):
        """Test the repr of a resource."""
        assert re.match(r"<.*\('%s'\)>" % str(self.rname), repr(self.instr))
        self.instr.close()
        assert re.match(r"<.*\('%s'\)>" % str(self.rname), repr(self.instr))

    def test_timeout(self):
        """Test setting the timeout attribute."""
        self.instr.timeout = None
        assert self.instr.timeout == float("+inf")
        assert (
            self.instr.get_visa_attribute(ResourceAttribute.timeout_value)
            == Timeouts.infinite
        )

        self.instr.timeout = 0.1
        assert self.instr.timeout == float(self.MINIMAL_TIMEOUT)
        assert (
            self.instr.get_visa_attribute(ResourceAttribute.timeout_value)
            == self.MINIMAL_TIMEOUT
        )

        self.instr.timeout = 10
        assert self.instr.timeout == 10
        assert self.instr.get_visa_attribute(ResourceAttribute.timeout_value) == 10

        with pytest.raises(ValueError):
            self.instr.timeout = 10000000000

        del self.instr.timeout
        assert self.instr.timeout == float("+inf")
        assert (
            self.instr.get_visa_attribute(ResourceAttribute.timeout_value)
            == Timeouts.infinite
        )

    def test_resource_info(self):
        """Test accessing the resource info."""
        rinfo = self.instr.resource_info
        assert rinfo.interface_type == getattr(
            InterfaceType, self.rname.interface_type.lower()
        )

        assert rinfo.interface_board_number == int(self.rname.board)
        assert rinfo.resource_class == self.rname.resource_class
        assert rinfo.resource_name == str(self.rname)

    def test_interface_type(self):
        """Test accessing the resource interface_type."""
        assert self.instr.interface_type == getattr(
            InterfaceType, self.rname.interface_type.lower()
        )

    def test_attribute_handling(self):
        """Test directly manipulating attributes ie not using descriptors.

        This should extended in subclasses to test a broader range of
        attributes.

        """
        self.instr.set_visa_attribute(ResourceAttribute.timeout_value, 10)
        assert self.instr.get_visa_attribute(ResourceAttribute.timeout_value) == 10
        assert self.instr.timeout == 10

        self.instr.set_visa_attribute(
            ResourceAttribute.timeout_value, Timeouts.immediate
        )
        assert (
            self.instr.get_visa_attribute(ResourceAttribute.timeout_value)
            == self.MINIMAL_TIMEOUT
        )
        assert self.instr.timeout == float(self.MINIMAL_TIMEOUT)

        self.instr.set_visa_attribute(
            ResourceAttribute.timeout_value, Timeouts.infinite
        )
        assert (
            self.instr.get_visa_attribute(ResourceAttribute.timeout_value)
            == Timeouts.infinite
        )
        assert self.instr.timeout == float("+inf")


class EventAwareResourceTestCaseMixin:
    """Mixing for resources supporting handling events."""

    def test_wait_on_event(self):
        """Test waiting on a VISA event.

        Should be implemented on subclasses, since the way to generate the
        event may be dependent on the resource type.

        """
        raise NotImplementedError()

    def test_managing_visa_handler(self):
        """Test using visa handlers.

        Should be implemented on subclasses, since the way to generate the
        event may be dependent on the resource type.

        """
        raise NotImplementedError()


class LockableResourceTestCaseMixin:
    """Mixing for resources supporting locking."""

    def test_shared_locking(self):
        """Test locking/unlocking a resource."""
        raise NotImplementedError()

    def test_exclusive_locking(self):
        """Test locking/unlocking a resource."""
        raise NotImplementedError()
