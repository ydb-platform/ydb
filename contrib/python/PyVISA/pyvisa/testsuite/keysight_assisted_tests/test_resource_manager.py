# -*- coding: utf-8 -*-
"""Test the capabilities of the ResourceManager."""

import gc
import logging
import re

import pytest

from pyvisa import InvalidSession, ResourceManager, VisaIOError, errors
from pyvisa.constants import AccessModes, InterfaceType, StatusCode
from pyvisa.highlevel import VisaLibraryBase
from pyvisa.rname import ResourceName
from pyvisa.testsuite import BaseTestCase

from . import RESOURCE_ADDRESSES, require_virtual_instr


@require_virtual_instr
class TestResourceManager:
    """Test the pyvisa ResourceManager."""

    def setup_method(self):
        """Create a ResourceManager with the default backend library."""
        self.rm = ResourceManager()

    def teardown_method(self):
        """Close the ResourceManager."""
        if self.rm is not None:
            self.rm.close()
            del self.rm
            gc.collect()

    def test_lifecycle(self, caplog):
        """Test creation and closing of the resource manager."""
        assert self.rm.session is not None
        assert self.rm.visalib is not None
        assert self.rm is self.rm.visalib.resource_manager
        assert not self.rm.list_opened_resources()

        assert self.rm.visalib is ResourceManager(self.rm.visalib).visalib

        with caplog.at_level(level=logging.DEBUG, logger="pyvisa"):
            self.rm.close()

        assert caplog.records

        with pytest.raises(InvalidSession):
            self.rm.session
        assert self.rm.visalib.resource_manager is None

    # This test is flaky
    @pytest.mark.skip
    def test_cleanup_on_del(self, caplog):
        """Test that deleting the rm does clean the VISA session"""
        # The test seems to assert what it should even though the coverage report
        # seems wrong
        rm = self.rm
        self.rm = None
        with caplog.at_level(logging.DEBUG, logger="pyvisa"):
            del rm
            gc.collect()

        assert "Closing ResourceManager" in caplog.records[0].message

    def test_resource_manager_unicity(self):
        """Test the resource manager is unique per backend as expected."""
        new_rm = ResourceManager()
        assert self.rm is new_rm
        assert self.rm.session == new_rm.session

    def test_str(self):
        """Test computing the string representation of the resource manager"""
        assert re.match(r"Resource Manager of .*", str(self.rm))
        self.rm.close()
        assert re.match(r"Resource Manager of .*", str(self.rm))

    def test_repr(self):
        """Test computing the repr of the resource manager"""
        assert re.match(r"<ResourceManager\(<.*>\)>", repr(self.rm))
        self.rm.close()
        assert re.match(r"<ResourceManager\(<.*>\)>", repr(self.rm))

    def test_last_status(self):
        """Test accessing the status of the last operation."""
        assert self.rm.last_status == StatusCode.success

        # Access the generic last status through the visalib
        assert self.rm.last_status == self.rm.visalib.last_status

        # Test accessing the status for an invalid session
        with pytest.raises(errors.Error) as cm:
            self.rm.visalib.get_last_status_in_session("_nonexisting_")
        assert "The session" in cm.exconly()

    def test_list_resource(self):
        """Test listing the available resources."""
        # Default settings
        resources = self.rm.list_resources()
        for v in (v for v in RESOURCE_ADDRESSES.values() if v.endswith("INSTR")):
            assert str(ResourceName.from_string(v)) in resources

        # All resources
        resources = self.rm.list_resources("?*")
        for v in RESOURCE_ADDRESSES.values():
            assert str(ResourceName.from_string(v)) in resources

    def test_accessing_resource_infos(self):
        """Test accessing resource infos."""
        rname = next(iter(RESOURCE_ADDRESSES.values()))
        rinfo_ext = self.rm.resource_info(rname)
        rinfo = self.rm.resource_info(rname, extended=False)

        rname = ResourceName().from_string(rname)
        assert rinfo_ext.interface_type == getattr(
            InterfaceType, rname.interface_type.lower()
        )
        assert rinfo_ext.interface_board_number == int(rname.board)
        assert rinfo_ext.resource_class == rname.resource_class
        assert rinfo_ext.resource_name == str(rname)

        assert rinfo.interface_type == getattr(
            InterfaceType, rname.interface_type.lower()
        )
        assert rinfo.interface_board_number == int(rname.board)

    def test_listing_resource_infos(self):
        """Test listing resource infos."""
        infos = self.rm.list_resources_info()

        for rname, rinfo_ext in infos.items():
            rname = ResourceName().from_string(rname)
            assert rinfo_ext.interface_type == getattr(
                InterfaceType, rname.interface_type.lower()
            )
            assert rinfo_ext.interface_board_number == int(rname.board)
            assert rinfo_ext.resource_class == rname.resource_class
            assert rinfo_ext.resource_name == str(rname)

    def test_opening_resource(self):
        """Test opening and closing resources."""
        rname = next(iter(RESOURCE_ADDRESSES.values()))
        rsc = self.rm.open_resource(rname, timeout=1234)

        # Check the resource is listed as opened and the attributes are right.
        assert rsc in self.rm.list_opened_resources()
        assert rsc.timeout == 1234

        # Close the rm to check that we close all resources.
        self.rm.close()

        assert not self.rm.list_opened_resources()
        with pytest.raises(InvalidSession):
            rsc.session

    def test_opening_resource_bad_open_timeout(self):
        """Test opening a resource with a non integer open_timeout."""
        rname = next(iter(RESOURCE_ADDRESSES.values()))

        with pytest.raises(ValueError) as cm:
            self.rm.open_resource(rname, open_timeout="")

        assert "integer (or compatible type)" in str(cm.exconly())

    def test_opening_resource_with_lock(self):
        """Test opening a locked resource"""
        rname = next(iter(RESOURCE_ADDRESSES.values()))
        rsc = self.rm.open_resource(rname, access_mode=AccessModes.exclusive_lock)
        assert len(self.rm.list_opened_resources()) == 1

        # Timeout when accessing a locked resource
        with pytest.raises(VisaIOError):
            self.rm.open_resource(rname, access_mode=AccessModes.exclusive_lock)
        assert len(self.rm.list_opened_resources()) == 1

        # Success to access an unlocked resource.
        rsc.unlock()
        with self.rm.open_resource(
            rname, access_mode=AccessModes.exclusive_lock
        ) as rsc2:
            assert rsc.session != rsc2.session
            assert len(self.rm.list_opened_resources()) == 2

    def test_opening_resource_specific_class(self):
        """Test opening a resource requesting a specific class."""
        rname = next(iter(RESOURCE_ADDRESSES.values()))
        with pytest.raises(TypeError):
            self.rm.open_resource(rname, resource_pyclass=object)

        assert len(self.rm.list_opened_resources()) == 0

    def test_open_resource_unknown_resource_type(self, caplog):
        """Test opening a resource for which no registered class exist."""
        rc = ResourceManager._resource_classes
        old = rc.copy()

        class FakeResource:
            def __init__(self, *args):
                raise RuntimeError()

        rc[(InterfaceType.unknown, "")] = FakeResource
        del rc[(InterfaceType.tcpip, "INSTR")]

        rm = ResourceManager()
        try:
            caplog.clear()
            with caplog.at_level(level=logging.DEBUG, logger="pyvisa"):
                with pytest.raises(RuntimeError):
                    rm.open_resource("TCPIP::192.168.0.1::INSTR")
            assert caplog.records
        finally:
            ResourceManager._resource_classes = old

    def test_opening_resource_unknown_attribute(self):
        """Test opening a resource and attempting to set an unknown attr."""
        rname = next(iter(RESOURCE_ADDRESSES.values()))
        with pytest.raises(ValueError):
            self.rm.open_resource(rname, unknown_attribute=None)

        assert len(self.rm.list_opened_resources()) == 0


@require_virtual_instr
class TestResourceParsing(BaseTestCase):
    """Test parsing resources using the builtin mechanism and the VISA lib.

    Those tests require that the interface exist (at least in Keysight
    implementation) so we cannot test arbitrary interfaces (PXI for example).

    """

    def setup_method(self):
        """Create a ResourceManager with the default backend library."""
        self.rm = ResourceManager()

    def teardown_method(self):
        """Close the ResourceManager."""
        del self.rm
        gc.collect()

    def test_parse_tcpip_instr(self):
        self._parse_test("TCPIP::192.168.200.200::INSTR")

    def test_parse_tcpip_socket(self):
        self._parse_test("TCPIP::192.168.200.200::7020::SOCKET")

    def _parse_test(self, rn):
        # Visa lib
        p = self.rm.visalib.parse_resource_extended(self.rm.session, rn)

        # Internal
        pb = VisaLibraryBase.parse_resource_extended(
            self.rm.visalib, self.rm.session, rn
        )
        assert p == pb

        # Non-extended parsing
        # Visa lib
        p = self.rm.visalib.parse_resource(self.rm.session, rn)

        # Internal
        pb = VisaLibraryBase.parse_resource(self.rm.visalib, self.rm.session, rn)
        assert p == pb
