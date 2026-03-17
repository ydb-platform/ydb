# -*- coding: utf-8 -*-
"""Test test the resource name parsing."""

import logging
from dataclasses import dataclass

import pytest
from typing_extensions import ClassVar

from pyvisa import constants, errors, rname
from pyvisa.testsuite import BaseTestCase


class TestInvalidResourceName(BaseTestCase):
    """Test the creation of InvalidResourceName errors."""

    def test_bad_syntax(self):
        """Test creating a bad syntax error."""
        e = rname.InvalidResourceName.bad_syntax("syntax", "resource")
        assert str(e) == "Could not parse 'resource'. The syntax is 'syntax'."

        e = rname.InvalidResourceName.bad_syntax("syntax", "resource", "ex")
        assert str(e) == "Could not parse 'resource'. The syntax is 'syntax' (ex)."

    def test_subclass_notfound(self):
        """Test creating a subclass not found error"""
        e = rname.InvalidResourceName.subclass_notfound("inter")
        assert str(e) == "Parser not found for: inter."

        e = rname.InvalidResourceName.subclass_notfound("inter", "resource")
        assert str(e) == "Could not parse 'resource'. Parser not found for: inter."

    def test_rc_notfound(self):
        """Test creating a resource not found error."""
        e = rname.InvalidResourceName.rc_notfound("inter")
        assert str(e) == "Resource class for inter not provided and default not found."

        e = rname.InvalidResourceName.rc_notfound("inter", "resource")
        assert str(e) == (
            "Could not parse 'resource'. "
            "Resource class for inter not provided and default not found."
        )


class TestRegisteringSubclass(BaseTestCase):
    """Test the validation of ResourceName subclass during registration."""

    def test_handling_duplicate(self):
        """Test we reject class for existing interface_type and resource class."""
        with pytest.raises(ValueError) as e:
            rname.register_subclass(rname.GPIBInstr)
        assert "Class already registered for" in e.exconly()

    def test_handling_duplicate_default(self) -> None:
        """Test we enforce the unicity of default resource class per interface."""
        with pytest.raises(ValueError) as e:

            @dataclass
            class R(rname.ResourceName):
                interface_type: ClassVar[str] = "TCPIP"
                resource_class: ClassVar[str] = "HISLIP"
                is_rc_optional: ClassVar[bool] = True

            rname.register_subclass(R)
        assert "Default already specified for" in e.exconly()


class TestResourceName(BaseTestCase):
    """Test error handling in ResourceName.

    This exercise creating a resource name from parts too which is hence not
    tested explicitely.

    """

    def test_creation_from_string(self):
        """Test error handling when creating a name from a string."""
        # No interface class registered
        with pytest.raises(rname.InvalidResourceName) as e:
            rname.ResourceName.from_string("RJ45::1")
        assert "unknown interface type" in e.exconly()

        # No default resource class registered
        with pytest.raises(rname.InvalidResourceName) as e:
            rname.ResourceName.from_string("PXI::1")
        assert "not provided and default not found" in e.exconly()

        # No resource class registered, this cannot really happen...
        try:
            rname._RESOURCE_CLASSES["GPIB"].add("RAW")
            with pytest.raises(rname.InvalidResourceName) as e:
                rname.ResourceName.from_string("GPIB::1::RAW")
            assert "Parser not found for:" in e.exconly()
        finally:
            rname._RESOURCE_CLASSES["GPIB"].remove("RAW")

        # Test handling less than required parts
        with pytest.raises(rname.InvalidResourceName) as e:
            rname.ResourceName.from_string("GPIB")
        assert "not enough parts" in e.exconly()

        # Test handling more than possible parts
        with pytest.raises(rname.InvalidResourceName) as e:
            rname.ResourceName.from_string("GPIB1::1::1::1::INSTR")
        assert "too many parts" in e.exconly()

        # Test handling missing mandatory part
        with pytest.raises(rname.InvalidResourceName) as e:
            rname.ResourceName.from_string("GPIB::::INSTR")
        assert "The syntax is" in e.exconly()

        # Test handling no part situation
        with pytest.raises(rname.InvalidResourceName) as e:
            rname.ResourceName.from_string("ASRL")
        assert "The syntax is" in e.exconly()

    def test_creation_from_kwargs(self):
        """Test error handling when creating a name from a kwargs."""
        # No interface class registered
        with pytest.raises(rname.InvalidResourceName) as e:
            rname.ResourceName.from_kwargs(interface_type="RJ45")
        assert "Unknown interface type:" in e.exconly()

        # No default resource class registered
        with pytest.raises(rname.InvalidResourceName) as e:
            rname.ResourceName.from_kwargs(interface_type="PXI", chassis_number="1")
        assert "not provided and default not found" in e.exconly()

        # No resource class registered
        with pytest.raises(rname.InvalidResourceName) as e:
            rname.ResourceName.from_kwargs(
                interface_type="GPIB", address=1, resource_class="RAW"
            )
        assert "Parser not found for:" in e.exconly()

        # Test bad resource from kwargs
        with pytest.raises(rname.InvalidResourceName) as e:
            rname.ResourceName.from_kwargs(
                interface_type="GPIB", resource_class="INSTR"
            )
        assert "required parameter" in e.exconly()

        # Test assembling from kwargs
        rn = rname.ResourceName.from_kwargs(interface_type="GPIB", primary_address="1")
        assert str(rn) == rname.to_canonical_name("GPIB::1")

    def test_accessing_interface_type(self):
        """Test converting the interface to a VISA constant"""
        types = constants.InterfaceType
        for it, itc in zip(
            ("ASRL", "USB", "GPIB", "TCPIP", "PXI", "VXI"),
            (types.asrl, types.usb, types.gpib, types.tcpip, types.pxi, types.vxi),
        ):
            rn = rname.ResourceName()
            rn.interface_type = it
            assert rn.interface_type_const == itc

        rn = rname.ResourceName()
        rn.interface_type = "none"
        assert rn.interface_type_const == constants.InterfaceType.unknown


class TestParsers(BaseTestCase):
    def _parse_test(self, rn, **kwargs):
        p = rname.ResourceName.from_string(rn)
        r = {k: getattr(p, k) for k in (*p._fields, "interface_type", "resource_class")}
        r["canonical_resource_name"] = rname.assemble_canonical_name(**r)
        assert r == kwargs, rn

    # @unittest.expectedFailure
    # def test_asrl_ethernet(self):
    #     self._parse_test('ASRL::1.2.3.4::2::INSTR',
    #                      interface_type='ASRL',
    #                      resource_class='INSTR',
    #                      port='2',
    #                      address='1.2.3.4')

    def test_asrl(self):
        self._parse_test(
            "ASRL1::INSTR",
            interface_type="ASRL",
            resource_class="INSTR",
            board="1",
            canonical_resource_name="ASRL1::INSTR",
        )

        self._parse_test(
            "ASRL1",
            interface_type="ASRL",
            resource_class="INSTR",
            board="1",
            canonical_resource_name="ASRL1::INSTR",
        )

    def test_gpib_instr(self):
        self._parse_test(
            "GPIB::1::1::INSTR",
            interface_type="GPIB",
            resource_class="INSTR",
            board="0",
            primary_address="1",
            secondary_address="1",
            canonical_resource_name="GPIB0::1::1::INSTR",
        )

        self._parse_test(
            "GPIB::1::INSTR",
            interface_type="GPIB",
            resource_class="INSTR",
            board="0",
            primary_address="1",
            secondary_address=None,
            canonical_resource_name="GPIB0::1::INSTR",
        )

        self._parse_test(
            "GPIB1::1::INSTR",
            interface_type="GPIB",
            resource_class="INSTR",
            board="1",
            primary_address="1",
            secondary_address=None,
            canonical_resource_name="GPIB1::1::INSTR",
        )

        self._parse_test(
            "GPIB1::1",
            interface_type="GPIB",
            resource_class="INSTR",
            board="1",
            primary_address="1",
            secondary_address=None,
            canonical_resource_name="GPIB1::1::INSTR",
        )

    def test_gpib_intf(self):
        self._parse_test(
            "GPIB::INTFC",
            interface_type="GPIB",
            resource_class="INTFC",
            board="0",
            canonical_resource_name="GPIB0::INTFC",
        )

        self._parse_test(
            "GPIB3::INTFC",
            interface_type="GPIB",
            resource_class="INTFC",
            board="3",
            canonical_resource_name="GPIB3::INTFC",
        )

    def test_prlgx_intf(self):
        self._parse_test(
            "PRLGX-TCPIP::1.2.3.4::INTFC",
            interface_type="PRLGX-TCPIP",
            resource_class="INTFC",
            host_address="1.2.3.4",
            port="1234",
            board="0",
            canonical_resource_name="PRLGX-TCPIP0::1.2.3.4::1234::INTFC",
        )

        self._parse_test(
            "PRLGX-TCPIP::169.254.1.80::23::INTFC",
            interface_type="PRLGX-TCPIP",
            resource_class="INTFC",
            host_address="169.254.1.80",
            port="23",
            board="0",
            canonical_resource_name="PRLGX-TCPIP0::169.254.1.80::23::INTFC",
        )

        self._parse_test(
            "PRLGX-TCPIP3::dev.company.com::INTFC",
            interface_type="PRLGX-TCPIP",
            resource_class="INTFC",
            host_address="dev.company.com",
            port="1234",
            board="3",
            canonical_resource_name="PRLGX-TCPIP3::dev.company.com::1234::INTFC",
        )

        self._parse_test(
            "PRLGX-ASRL2::/dev/cu.usbserial-5678::INTFC",
            interface_type="PRLGX-ASRL",
            resource_class="INTFC",
            serial_device="/dev/cu.usbserial-5678",
            board="2",
            canonical_resource_name="PRLGX-ASRL2::/dev/cu.usbserial-5678::INTFC",
        )

        self._parse_test(
            "PRLGX-ASRL::asrl1::INTFC",
            interface_type="PRLGX-ASRL",
            resource_class="INTFC",
            serial_device="asrl1",
            board="0",
            canonical_resource_name="PRLGX-ASRL0::asrl1::INTFC",
        )

    def test_tcpip_intr(self):
        self._parse_test(
            "TCPIP::192.168.134.102",
            interface_type="TCPIP",
            resource_class="INSTR",
            host_address="192.168.134.102",
            board="0",
            lan_device_name="inst0",
            canonical_resource_name="TCPIP0::192.168.134.102::inst0::INSTR",
        )

        self._parse_test(
            "TCPIP::dev.company.com::INSTR",
            interface_type="TCPIP",
            resource_class="INSTR",
            host_address="dev.company.com",
            board="0",
            lan_device_name="inst0",
            canonical_resource_name="TCPIP0::dev.company.com::inst0::INSTR",
        )

        self._parse_test(
            "TCPIP3::dev.company.com::inst3::INSTR",
            interface_type="TCPIP",
            resource_class="INSTR",
            host_address="dev.company.com",
            board="3",
            lan_device_name="inst3",
            canonical_resource_name="TCPIP3::dev.company.com::inst3::INSTR",
        )

        self._parse_test(
            "TCPIP3::1.2.3.4::inst3::INSTR",
            interface_type="TCPIP",
            resource_class="INSTR",
            host_address="1.2.3.4",
            board="3",
            lan_device_name="inst3",
            canonical_resource_name="TCPIP3::1.2.3.4::inst3::INSTR",
        )

    def test_vicp_intr(self):
        self._parse_test(
            "VICP::192.168.134.102",
            interface_type="VICP",
            resource_class="INSTR",
            host_address="192.168.134.102",
            _unused=None,
            canonical_resource_name="VICP::192.168.134.102::INSTR",
        )

        self._parse_test(
            "VICP::dev.company.com::INSTR",
            interface_type="VICP",
            resource_class="INSTR",
            host_address="dev.company.com",
            _unused=None,
            canonical_resource_name="VICP::dev.company.com::INSTR",
        )

        self._parse_test(
            "VICP::dev.company.com::INSTR",
            interface_type="VICP",
            resource_class="INSTR",
            host_address="dev.company.com",
            _unused=None,
            canonical_resource_name="VICP::dev.company.com::INSTR",
        )

        self._parse_test(
            "VICP::1.2.3.4::INSTR",
            interface_type="VICP",
            resource_class="INSTR",
            host_address="1.2.3.4",
            _unused=None,
            canonical_resource_name="VICP::1.2.3.4::INSTR",
        )

    def test_tcpip_socket(self):
        self._parse_test(
            "TCPIP::1.2.3.4::999::SOCKET",
            interface_type="TCPIP",
            resource_class="SOCKET",
            host_address="1.2.3.4",
            board="0",
            port="999",
            canonical_resource_name="TCPIP0::1.2.3.4::999::SOCKET",
        )

        self._parse_test(
            "TCPIP2::1.2.3.4::999::SOCKET",
            interface_type="TCPIP",
            resource_class="SOCKET",
            host_address="1.2.3.4",
            board="2",
            port="999",
            canonical_resource_name="TCPIP2::1.2.3.4::999::SOCKET",
        )

    def test_usb_instr(self):
        self._parse_test(
            "USB::0x1234::125::A22-5::INSTR",
            interface_type="USB",
            resource_class="INSTR",
            manufacturer_id="0x1234",
            model_code="125",
            serial_number="A22-5",
            board="0",
            usb_interface_number="0",
            canonical_resource_name="USB0::0x1234::125::A22-5::0::INSTR",
        )

        self._parse_test(
            "USB2::0x1234::125::A22-5::INSTR",
            interface_type="USB",
            resource_class="INSTR",
            manufacturer_id="0x1234",
            model_code="125",
            serial_number="A22-5",
            board="2",
            usb_interface_number="0",
            canonical_resource_name="USB2::0x1234::125::A22-5::0::INSTR",
        )

        self._parse_test(
            "USB::0x1234::125::A22-5",
            interface_type="USB",
            resource_class="INSTR",
            manufacturer_id="0x1234",
            model_code="125",
            serial_number="A22-5",
            board="0",
            usb_interface_number="0",
            canonical_resource_name="USB0::0x1234::125::A22-5::0::INSTR",
        )

        self._parse_test(
            "USB::0x1234::125::A22-5::3::INSTR",
            interface_type="USB",
            resource_class="INSTR",
            manufacturer_id="0x1234",
            model_code="125",
            serial_number="A22-5",
            board="0",
            usb_interface_number="3",
            canonical_resource_name="USB0::0x1234::125::A22-5::3::INSTR",
        )

        self._parse_test(
            "USB2::0x1234::125::A22-5::3::INSTR",
            interface_type="USB",
            resource_class="INSTR",
            manufacturer_id="0x1234",
            model_code="125",
            serial_number="A22-5",
            board="2",
            usb_interface_number="3",
            canonical_resource_name="USB2::0x1234::125::A22-5::3::INSTR",
        )

        self._parse_test(
            "USB1::0x1234::125::A22-5::3",
            interface_type="USB",
            resource_class="INSTR",
            manufacturer_id="0x1234",
            model_code="125",
            serial_number="A22-5",
            board="1",
            usb_interface_number="3",
            canonical_resource_name="USB1::0x1234::125::A22-5::3::INSTR",
        )

    def test_usb_raw(self):
        self._parse_test(
            "USB::0x1234::125::A22-5::RAW",
            interface_type="USB",
            resource_class="RAW",
            manufacturer_id="0x1234",
            model_code="125",
            serial_number="A22-5",
            board="0",
            usb_interface_number="0",
            canonical_resource_name="USB0::0x1234::125::A22-5::0::RAW",
        )

        self._parse_test(
            "USB2::0x1234::125::A22-5::RAW",
            interface_type="USB",
            resource_class="RAW",
            manufacturer_id="0x1234",
            model_code="125",
            serial_number="A22-5",
            board="2",
            usb_interface_number="0",
            canonical_resource_name="USB2::0x1234::125::A22-5::0::RAW",
        )

        self._parse_test(
            "USB2::0x1234::125::A22-5::3::RAW",
            interface_type="USB",
            resource_class="RAW",
            manufacturer_id="0x1234",
            model_code="125",
            serial_number="A22-5",
            board="2",
            usb_interface_number="3",
            canonical_resource_name="USB2::0x1234::125::A22-5::3::RAW",
        )


class TestFilters(BaseTestCase):
    CHECK_NO_WARNING = False

    run_list = (
        "GPIB0::8::INSTR",
        "TCPIP0::localhost:1111::inst0::INSTR",
        "ASRL1::INSTR",
        "USB1::0x1111::0x2222::0x4445::0::RAW",
        "USB0::0x1112::0x2223::0x1234::0::INSTR",
        "TCPIP0::192.168.0.1::inst1::INSTR",
        "TCPIP0::localhost::10001::SOCKET",
        "GPIB9::7::1::INSTR",
        "ASRL11::INSTR",
        "ASRL2::INSTR",
        "GPIB::INTFC",
        "PXI::1::BACKPLANE",
        "PXI::MEMACC",
        "VXI::1::BACKPLANE",  # should work without the one
        "VXI::1::INSTR",
        "VXI::SERVANT",
    )

    def _test_filter(self, expr, *correct):
        ok = tuple(self.run_list[n] for n in correct)
        assert rname.filter(self.run_list, expr) == ok

    def _test_filter2(self, expr, *correct):
        class MockedResource(object):
            def get_visa_attribute(self, attr):
                if attr == "VI_test":
                    return 1
                else:
                    raise ValueError()

            def close(self):
                pass

        ok = tuple(self.run_list[n] for n in correct)
        filtered = rname.filter2(self.run_list, expr, lambda x: MockedResource())
        assert filtered == ok

    def test_filter(self, caplog):
        self._test_filter("?*::INSTR", 0, 1, 2, 4, 5, 7, 8, 9, 14)
        self._test_filter("GPIB?+INSTR", 0, 7)
        self._test_filter("GPIB[0-8]*::?*INSTR", 0)
        self._test_filter("GPIB[^0]::?*INSTR", 7)
        self._test_filter("ASRL1+::INSTR", 2, 8)
        self._test_filter("(GPIB|VXI)?*INSTR", 0, 7, 14)
        with caplog.at_level(logging.DEBUG):
            self._test_filter("?*{}", *tuple(range(len(self.run_list))))
        assert (
            "optional part of the query expression not supported."
            in caplog.records[0].message
        )
        # Not sure why this is needed
        self._test_handler = None

    def test_filter2_no_optional_clause(self):
        self._test_filter2("?*::INSTR", 0, 1, 2, 4, 5, 7, 8, 9, 14)
        self._test_filter2("GPIB?+INSTR", 0, 7)
        self._test_filter2("GPIB[0-8]*::?*INSTR", 0)
        self._test_filter2("GPIB[^0]::?*INSTR", 7)
        self._test_filter2("ASRL1+::INSTR", 2, 8)
        self._test_filter2("(GPIB|VXI)?*INSTR", 0, 7, 14)

    def test_filter2_optional_clause_no_connection(self):
        self._test_filter2("?*::INSTR{!(VI_ATTR_INTF_NUM == 0)}", 2, 7, 8, 9)
        self._test_filter2('?*{VI_ATTR_MANF_ID == "0x1111"}', 3)
        self._test_filter2('?*{VI_ATTR_MODEL_CODE == "0x2223"}', 4)
        self._test_filter2('?*{VI_ATTR_USB_SERIAL_NUM == "0x1234"}', 4)
        self._test_filter2("?*{VI_ATTR_USB_INTFC_NUM == 0}", 4)
        self._test_filter2('?*{VI_ATTR_TCPIP_ADDR == "localhost"}', 6)
        self._test_filter2('?*{VI_ATTR_TCPIP_DEVICE_NAME == "inst1"}', 5)
        self._test_filter2("?*{VI_ATTR_TCPIP_PORT == 10001}", 6)
        self._test_filter2("?*{VI_ATTR_GPIB_PRIMARY_ADDR == 8}", 0)
        self._test_filter2("?*{VI_ATTR_GPIB_SECONDARY_ADDR == 1}", 7)
        self._test_filter2(
            "?*{VI_ATTR_GPIB_SECONDARY_ADDR == %d}" % constants.VI_NO_SEC_ADDR, 0
        )
        self._test_filter2("?*{VI_ATTR_PXI_CHASSIS == 1}", 11)
        self._test_filter2("?*{VI_ATTR_MAINFRAME_LA == 1}", 13, 14)
        self._test_filter2("?*{VI_ATTR_MAINFRAME_LA == 1 && VI_test == 1}", 13, 14)

    def test_bad_filter(self):
        with pytest.raises(errors.VisaIOError) as e:
            rname.filter([], "?*(")
        assert "VI_ERROR_INV_EXPR" in e.exconly()

    def test_bad_filter2(self):
        with pytest.raises(errors.VisaIOError) as e:
            rname.filter2([], "?*{", lambda x: None)
        assert "VI_ERROR_INV_EXPR" in e.exconly()
