# -*- coding: utf-8 -*-
"""Test the TCPIP based resources."""

import pytest

from pyvisa import constants, errors

from . import copy_func, require_virtual_instr
from .messagebased_resource_utils import (
    EventAwareMessagebasedResourceTestCaseMixin,
    LockableMessagedBasedResourceTestCaseMixin,
    MessagebasedResourceTestCase,
)


@require_virtual_instr
class TestTCPIPInstr(
    LockableMessagedBasedResourceTestCaseMixin,
    EventAwareMessagebasedResourceTestCaseMixin,
    MessagebasedResourceTestCase,
):
    """Test pyvisa against a TCPIP INSTR resource."""

    #: Type of resource being tested in this test case.
    #: See RESOURCE_ADDRESSES in the __init__.py file of this package for
    #: acceptable values
    RESOURCE_TYPE = "TCPIP::INSTR"

    #: Minimal timeout value accepted by the resource. When setting the timeout
    #: to VI_TMO_IMMEDIATE, Visa (Keysight at least) may actually use a
    #: different value depending on the values supported by the resource.
    MINIMAL_TIMEOUT = 10

    def test_io_prot_attr(self):
        """Test getting/setting the io prot attribute.

        We would need to spy on the transaction to ensure we are sending a
        string instead of using the lower level mechanism.

        """
        try:
            self.instr.read_stb()
            # XXX note sure what is the actual issue here
            with pytest.raises(errors.VisaIOError):
                self.instr.set_visa_attribute(
                    constants.VI_ATTR_IO_PROT, constants.IOProtocol.hs488
                )
            # self.instr.read_stb()
            # assert (
            #     self.instr.get_visa_attribute(constants.VI_ATTR_IO_PROT)
            #     == constants.IOProtocol.hs488
            # )
        finally:
            self.instr.set_visa_attribute(
                constants.VI_ATTR_IO_PROT, constants.IOProtocol.normal
            )


@require_virtual_instr
class TestTCPIPSocket(MessagebasedResourceTestCase):
    """Test pyvisa against a TCPIP SOCKET resource."""

    #: Type of resource being tested in this test case.
    #: See RESOURCE_ADDRESSES in the __init__.py file of this package for
    #: acceptable values
    RESOURCE_TYPE = "TCPIP::SOCKET"

    #: Minimal timeout value accepted by the resource. When setting the timeout
    #: to VI_TMO_IMMEDIATE, Visa (Keysight at least) may actually use a
    #: different value depending on the values supported by the resource.
    MINIMAL_TIMEOUT = 0

    # Copy functions since the marker is applied in-place
    test_write_raw_read_bytes = pytest.mark.xfail(
        copy_func(MessagebasedResourceTestCase.test_write_raw_read_bytes)
    )
    test_write_raw_read_raw = pytest.mark.xfail(
        copy_func(MessagebasedResourceTestCase.test_write_raw_read_raw)
    )
    test_write_read = pytest.mark.xfail(
        copy_func(MessagebasedResourceTestCase.test_write_read)
    )
    test_write_ascii_values = pytest.mark.xfail(
        copy_func(MessagebasedResourceTestCase.test_write_ascii_values)
    )
    test_write_binary_values = pytest.mark.xfail(
        copy_func(MessagebasedResourceTestCase.test_write_binary_values)
    )
    test_read_ascii_values = pytest.mark.xfail(
        copy_func(MessagebasedResourceTestCase.test_read_ascii_values)
    )
    test_read_binary_values = pytest.mark.xfail(
        copy_func(MessagebasedResourceTestCase.test_read_binary_values)
    )
    test_read_query_binary_values_invalid_header = pytest.mark.xfail(
        copy_func(
            MessagebasedResourceTestCase.test_read_query_binary_values_invalid_header
        )
    )
    test_read_binary_values_unreported_length = pytest.mark.xfail(
        copy_func(
            MessagebasedResourceTestCase.test_read_binary_values_unreported_length
        )
    )
    test_read_binary_values_empty = pytest.mark.xfail(
        copy_func(MessagebasedResourceTestCase.test_read_binary_values_empty)
    )
    test_delay_in_query_ascii = pytest.mark.xfail(
        copy_func(MessagebasedResourceTestCase.test_delay_in_query_ascii)
    )
    test_instrument_wide_delay_in_query_binary = pytest.mark.xfail(
        copy_func(
            MessagebasedResourceTestCase.test_instrument_wide_delay_in_query_binary
        )
    )
    test_delay_args_in_query_binary = pytest.mark.xfail(
        copy_func(MessagebasedResourceTestCase.test_delay_args_in_query_binary)
    )
    test_no_delay_args_in_query_binary = pytest.mark.xfail(
        copy_func(MessagebasedResourceTestCase.test_no_delay_args_in_query_binary)
    )
    test_stb = pytest.mark.skip(copy_func(MessagebasedResourceTestCase.test_stb))
