# -*- coding: utf-8 -*-
"""Common test case for all message based resources."""

import ctypes
import gc
import logging
import time
from itertools import product
from types import ModuleType
from typing import Optional, Union

import pytest

from pyvisa import constants, errors
from pyvisa.constants import EventType, ResourceAttribute
from pyvisa.resources import Resource

from .resource_utils import (
    EventAwareResourceTestCaseMixin,
    LockableResourceTestCaseMixin,
    ResourceTestCase,
)

np: Optional[ModuleType]
try:
    import numpy

    np = numpy
except ImportError:
    np = None


class DummyMonitoringDevice:
    """A test object that implements the progress bar interface"""

    def __init__(self, total_bytes: int) -> None:
        self.last_update: int = 0
        self.total_bytes: int = total_bytes

    def update(self, size: Union[int, None]) -> None:
        if size is not None:
            self.last_update += size


class EventHandler:
    """Event handler."""

    def __init__(self) -> None:
        self.event_success = False
        self.srq_success = False
        self.io_completed = False
        self.handle = None
        self.session = None

    def handle_event(self, session, event_type, event, handle=None):
        """Event handler

        Ctypes handler are expected to return an interger.

        """
        self.session = session
        self.handle = handle
        if event_type == EventType.service_request:
            self.event_success = True
            self.srq_success = True
            return 0
        if event_type == EventType.io_completion:
            self.event_success = True
            self.io_completed = True
            return 0
        else:
            self.event_success = True
            return 0

    def simplified_handler(self, resource, event, handle=None):
        """Simplified handler that can be wrapped."""
        self.session = resource.session
        self.handle = handle
        event_type = event.event_type
        if event_type == EventType.service_request:
            self.event_success = True
            self.srq_success = True
            return None
        elif event_type == EventType.io_completion:
            self.event_success = True
            self.io_completed = True
            return None
        else:
            self.event_success = True
            return None


class MessagebasedResourceTestCase(ResourceTestCase):
    """Base test case for all message based resources."""

    #: Type of resource being tested in this test case.
    #: See RESOURCE_ADDRESSES in the __init__.py file of this package for
    #: acceptable values
    RESOURCE_TYPE = ""

    # Any test involving communication involve to first write to glider the
    # data then request it to send it back

    def setup_method(self):
        """Create a resource using the address matching the type."""
        super().setup_method()
        self.instr.write_termination = "\n"
        self.instr.read_termination = "\n"
        self.instr.timeout = 100

    def compare_user_handle(self, h1, h2):
        """Function comparing to user handle as passed to a callback.

        We need such an indirection because we cannot safely always return
        a Python object and most ctypes object do not compare equal.

        """
        if isinstance(h1, ctypes.Structure):
            return h1 == h2
        elif hasattr(h1, "value"):
            return h1.value == h2.value
        else:  # assume an array
            return all((i == j for i, j in zip(h1, h2)))

    def test_encoding(self):
        """Tets setting the string encoding."""
        assert self.instr.encoding == "ascii"
        self.instr.encoding = "utf-8"

        with pytest.raises(LookupError):
            self.instr.encoding = "test"

    def test_termchars(self):
        """Test modifying the termchars."""
        # Write termination
        self.instr.write_termination = "\r\n"
        assert self.instr.write_termination == "\r\n"

        self.instr.read_termination = "\r\0"
        assert self.instr.read_termination == "\r\0"
        assert self.instr.get_visa_attribute(ResourceAttribute.termchar) == ord("\0")
        assert self.instr.get_visa_attribute(ResourceAttribute.termchar_enabled)

        # Disable read termination
        self.instr.read_termination = None
        assert self.instr.get_visa_attribute(ResourceAttribute.termchar) == ord("\n")
        assert not self.instr.get_visa_attribute(ResourceAttribute.termchar_enabled)

        # Ban repeated term chars
        with pytest.raises(ValueError):
            self.instr.read_termination = "\n\n"

    def test_write_raw_read_bytes(self):
        """Test writing raw data and reading a specific number of bytes."""
        # Reading all bytes at once
        self.instr.write_raw(b"RECEIVE\n")
        self.instr.write_raw(b"test\n")
        count = self.instr.write_raw(b"SEND\n")
        assert count == 5
        self.instr.flush(constants.VI_READ_BUF)
        msg = self.instr.read_bytes(5, chunk_size=2)
        assert msg == b"test\n"

        # Reading one byte at a time
        self.instr.write_raw(b"RECEIVE\n")
        self.instr.write_raw(b"test\n")
        self.instr.write_raw(b"SEND\n")
        for ch in b"test\n":
            assert self.instr.read_bytes(1) == ch.to_bytes(1, "little")

        # Breaking on termchar
        self.instr.read_termination = "\r"
        self.instr.write_raw(b"RECEIVE\n")
        self.instr.write_raw(b"te\rst\r\n")
        self.instr.write_raw(b"SEND\n")
        assert self.instr.read_bytes(100, break_on_termchar=True) == b"te\r"
        assert self.instr.read_bytes(100, break_on_termchar=True) == b"st\r"
        assert self.instr.read_bytes(1) == b"\n"

        # Breaking on end of message
        self.instr.read_termination = "\n"
        self.instr.write_raw(b"RECEIVE\n")
        self.instr.write_raw(b"test\n")
        self.instr.write_raw(b"SEND\n")
        assert self.instr.read_bytes(100, break_on_termchar=True) == b"test\n"

    def test_handling_exception_in_read_bytes(self, caplog):
        """Test handling exception in read_bytes (monkeypatching)"""

        def false_read(session, size):
            raise errors.VisaIOError(constants.VI_ERROR_ABORT)

        read = self.instr.visalib.read
        self.instr.visalib.read = false_read
        with caplog.at_level(logging.DEBUG):
            try:
                self.instr.read_bytes(1)
            except errors.VisaIOError:
                pass
            finally:
                self.instr.visalib.read = read
        assert "- exception while reading:" in caplog.records[1].message

    def test_write_raw_read_raw(self):
        """Test writing raw data and reading an answer."""
        self.instr.write_raw(b"RECEIVE\n")
        self.instr.write_raw(b"test\n")
        self.instr.write_raw(b"SEND\n")
        assert self.instr.read_raw(size=2) == b"test\n"

    def test_clear(self):
        """Test clearing the incoming buffer."""
        self.instr.write_raw(b"RECEIVE\n")
        self.instr.write_raw(b"test\n")
        self.instr.write_raw(b"SEND\n")
        self.instr.clear()
        self.instr.timeout = 10
        with pytest.raises(errors.VisaIOError):
            self.instr.read_raw()

    def test_write_read(self):
        """Test writing and reading."""
        self.instr.write_termination = "\n"
        self.instr.read_termination = "\r\n"
        self.instr.write("RECEIVE")
        with pytest.warns(UserWarning):
            self.instr.write("test\r\n")
        count = self.instr.write("SEND")
        assert count == 5
        assert self.instr.read() == "test"

        # Missing termination chars
        self.instr.read_termination = "\r\n"
        self.instr.write("RECEIVE")
        self.instr.write("test")
        self.instr.write("SEND")
        with pytest.warns(Warning):
            assert self.instr.read() == "test\n"

        # Dynamic termination
        self.instr.write_termination = "\r"
        self.instr.write("RECEIVE\n", termination=False)
        self.instr.write("test\r", termination="\n")
        self.instr.write("SEND", termination="\n")
        assert self.instr.read(termination="\r") == "test"

        # Test query
        self.instr.write_termination = "\n"
        self.instr.write("RECEIVE")
        self.instr.write("test\r")
        tic = time.time()
        assert self.instr.query("SEND", delay=0.5) == "test"
        assert time.time() - tic > 0.49

        # Test handling repeated term char
        self.instr.read_termination = "\n"
        for char in ("\r", None):
            self.instr.write_termination = "\n" if char else "\r"
            self.instr.write("RECEIVE", termination="\n")
            with pytest.warns(Warning):
                self.instr.write("test\r", termination=char)
            self.instr.write("", termination="\n")
            self.instr.write("SEND", termination="\n")
            assert self.instr.read() == "test\r\r"

        # TODO not sure how to test encoding

    def test_handling_exception_in_read_raw(self, caplog):
        """Test handling exception in read_bytes (monkeypatching)"""

        def false_read(session, size):
            raise errors.VisaIOError(constants.VI_ERROR_ABORT)

        read = self.instr.visalib.read
        self.instr.visalib.read = false_read
        with caplog.at_level(logging.DEBUG):
            try:
                self.instr.read()
            except errors.VisaIOError:
                pass
            finally:
                self.instr.visalib.read = read

        assert caplog.records

    def test_write_ascii_values(self):
        """Test writing ascii values."""
        # Standard separator
        values = [1, 2, 3, 4, 5]
        self.instr.write("RECEIVE")
        count = self.instr.write_ascii_values("", values, "d")
        assert count == 10
        self.instr.write("SEND")
        assert self.instr.read() == "1,2,3,4,5"

        # Non standard separator and termination
        self.instr.write_termination = "\r"
        self.instr.write("RECEIVE", termination="\n")
        self.instr.write_ascii_values("", values, "d", separator=";", termination=False)
        self.instr.write("", termination="\n")
        self.instr.write("SEND", termination="\n")
        assert self.instr.read() == "1;2;3;4;5"

        # Test handling repeated term char
        for char in ("\r", None):
            self.instr.write_termination = "\n" if char else "\r"
            self.instr.write("RECEIVE", termination="\n")
            with pytest.warns(Warning):
                values = [1, 2, 3, 4, 5]
                self.instr.write_ascii_values(
                    "\r", values, "s", separator=";", termination=char
                )
            self.instr.write("", termination="\n")
            self.instr.write("SEND", termination="\n")
            assert self.instr.read() == "\r1;2;3;4;5\r"

    @pytest.mark.parametrize(
        "hfmt, prefix",
        list(zip(("ieee", "hp", "empty"), (b"#212", b"#A\x0c\x00", b""))),
    )
    def test_write_binary_values(self, hfmt, prefix):
        """Test writing binary data."""
        values = [1, 2, 3, 4, 5, 6]
        self.instr.write_termination = "\n"
        self.instr.write("RECEIVE")
        count = self.instr.write_binary_values("", values, "h", header_fmt=hfmt)
        # Each interger encoded as h uses 2 bytes
        assert count == len(prefix) + 12 + 1
        self.instr.write("SEND")
        msg = self.instr.read_bytes(13 + len(prefix))
        assert msg == prefix + b"\x01\x00\x02\x00\x03\x00\x04\x00\x05\x00\x06\x00\n"

        if hfmt == "hp":
            fl_prefix = prefix[0:2] + prefix[-2::][::-1]
        else:
            fl_prefix = prefix
        self.instr.write_termination = "\r"
        self.instr.write("RECEIVE", termination="\n")
        self.instr.write_binary_values(
            "", values, "h", is_big_endian=True, termination=False, header_fmt=hfmt
        )
        self.instr.write("", termination="\n")
        self.instr.write("SEND", termination="\n")
        assert (
            self.instr.read_bytes(13 + len(prefix))
            == fl_prefix + b"\x00\x01\x00\x02\x00\x03\x00\x04\x00\x05\x00\x06\n"
        )

        # Test handling repeated term char
        for char in ("\r", None):
            self.instr.write_termination = "\n" if char else "\r"
            self.instr.write("RECEIVE", termination="\n")
            with pytest.warns(Warning):
                self.instr.write_binary_values(
                    "\r", values, "h", header_fmt=hfmt, termination=char
                )
            self.instr.write("", termination="\n")
            self.instr.write("SEND", termination="\n")
            msg = self.instr.read()
            assert (
                msg
                == "\r"
                + prefix.decode("ascii")
                + "\x01\x00\x02\x00\x03\x00\x04\x00\x05\x00\x06\x00\r"
            )

        # Wrong header format
        with pytest.raises(ValueError):
            self.instr.write_binary_values("", values, "h", header_fmt="zxz")

    # Without and with trailing comma
    @pytest.mark.parametrize("msg", ["1,2,3,4,5", "1,2,3,4,5,"])
    def test_read_ascii_values(self, msg):
        """Test reading ascii values."""
        # Standard separator
        self.instr.write("RECEIVE")
        self.instr.write(msg)
        self.instr.write("SEND")
        values = self.instr.read_ascii_values()
        assert isinstance(values[0], float)
        assert values == [1.0, 2.0, 3.0, 4.0, 5.0]

        # Non standard separator and termination
        self.instr.write("RECEIVE")
        self.instr.write(msg.replace(",", ";"))
        tic = time.time()
        values = self.instr.query_ascii_values(
            "SEND", converter="d", separator=";", delay=0.5
        )
        assert time.time() - tic > 0.5
        assert isinstance(values[0], int)
        assert values == [1, 2, 3, 4, 5]

        # Numpy container
        if np:
            self.instr.write("RECEIVE")
            self.instr.write(msg)
            self.instr.write("SEND")
            values = self.instr.read_ascii_values(container=np.array)
            expected = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
            assert values.dtype is expected.dtype
            np.testing.assert_array_equal(values, expected)

    @pytest.mark.parametrize(
        "hfmt, use_pb", list(product(("ieee", "hp"), (False, True)))
    )
    def test_read_binary_values(self, hfmt, use_pb):
        """Test reading binary data."""
        # TODO test handling binary decoding issue (troublesome)
        self.instr.read_termination = "\r"
        # 3328 in binary short is \x00\r this way we can interrupt the
        # transmission midway to test some corner cases
        data = [1, 2, 3328, 3, 4, 5, 6, 7]
        data_length = 2 * len(data)
        # Calculate header length based on header format
        header_length = len(f"{data_length}") + 2 if hfmt == "ieee" else 4
        monitor = (
            DummyMonitoringDevice(
                data_length + header_length + 1
            )  # 1 for the term char
            if use_pb
            else None
        )

        self.instr.write("RECEIVE")
        self.instr.write_binary_values(
            "", data, "h", header_fmt=hfmt, termination="\r\n"
        )
        self.instr.write("SEND")
        new = self.instr.read_binary_values(
            datatype="h",
            is_big_endian=False,
            header_fmt=hfmt,
            expect_termination=True,
            chunk_size=8,
            monitoring_interface=monitor,
        )
        self.instr.read_bytes(1)
        assert data == new
        if use_pb:
            assert monitor.last_update == monitor.total_bytes

        self.instr.write("RECEIVE")
        self.instr.write_binary_values(
            "", data, "h", header_fmt=hfmt, is_big_endian=True
        )
        monitor = DummyMonitoringDevice(data_length + header_length) if use_pb else None
        new = self.instr.query_binary_values(
            "SEND",
            datatype="h",
            header_fmt=hfmt,
            is_big_endian=True,
            expect_termination=False,
            chunk_size=8,
            container=np.array if np else list,
            monitoring_interface=monitor,
        )
        self.instr.read_bytes(1)
        if np:
            np.testing.assert_array_equal(new, np.array(data, dtype=np.int16))
        else:
            assert data == new

        if use_pb:
            assert monitor.last_update == monitor.total_bytes

    def test_read_query_binary_values_invalid_header(self):
        """Test we properly handle an invalid header."""
        data = [1, 2, 3328, 3, 4, 5, 6, 7]
        self.instr.write("RECEIVE")
        self.instr.write_binary_values(
            "", data, "h", header_fmt="ieee", is_big_endian=True
        )
        self.instr.write("SEND")
        with pytest.raises(ValueError):
            self.instr.read_binary_values(
                datatype="h",
                is_big_endian=False,
                header_fmt="invalid",
                expect_termination=True,
                chunk_size=8,
            )

        self.instr.write("RECEIVE")
        self.instr.write_binary_values(
            "", data, "h", header_fmt="ieee", is_big_endian=True
        )
        with pytest.raises(ValueError):
            self.instr.query_binary_values(
                "*IDN",
                datatype="h",
                is_big_endian=False,
                header_fmt="invalid",
                expect_termination=True,
                chunk_size=8,
            )

    # Not sure how to test this
    @pytest.mark.skip
    def test_handling_malformed_binary(self):
        """ """
        pass

    @pytest.mark.parametrize("hfmt, header", list(zip(("ieee", "empty"), ("#0", ""))))
    def test_read_binary_values_unreported_length(self, hfmt, header):
        """Test reading binary data."""
        self.instr.read_termination = "\r"
        # 3328 in binary short is \x00\r this way we can interrupt the
        # transmission midway to test some corner cases
        data = [1, 2, 3328, 3, 4, 5]

        self.instr.write("RECEIVE")
        self.instr.write(
            header + "\x01\x00\x02\x00\x00\r\x03\x00\x04\x00\x05\x00",
            termination="\r\n",
        )
        self.instr.write("SEND")
        new = self.instr.read_binary_values(
            datatype="h",
            is_big_endian=False,
            header_fmt=hfmt,
            expect_termination=True,
            chunk_size=6,
            data_points=6,
        )
        self.instr.read_bytes(1)
        assert data == new

        self.instr.write("RECEIVE")
        self.instr.write(
            header + "\x00\x01\x00\x02\r\x00\x00\x03\x00\x04\x00\x05",
            termination="\r\n",
        )
        new = self.instr.query_binary_values(
            "SEND",
            datatype="h",
            header_fmt=hfmt,
            is_big_endian=True,
            expect_termination=False,
            chunk_size=6,
            container=np.array if np else list,
            data_points=6,
        )
        self.instr.read_bytes(1)
        if np:
            np.testing.assert_array_equal(new, np.array(data, dtype=np.int16))
        else:
            assert data == new

        # Check we do error on unreported/unspecified length
        self.instr.write("RECEIVE")
        self.instr.write(
            header + "\x01\x00\x02\x00\x00\r\x03\x00\x04\x00\x05\x00",
            termination="\r\n",
        )
        self.instr.write("SEND")
        with pytest.raises(ValueError):
            self.instr.read_binary_values(
                datatype="h",
                is_big_endian=False,
                header_fmt=hfmt,
                expect_termination=True,
                chunk_size=6,
            )

    def test_read_binary_values_empty(self):
        """Test reading binary data."""
        self.instr.write("RECEIVE")
        self.instr.write("#10")
        self.instr.write("SEND")
        new = self.instr.read_binary_values(
            datatype="h",
            is_big_endian=False,
            header_fmt="ieee",
            expect_termination=True,
            chunk_size=6,
        )
        assert not new

        if np:
            self.instr.write("RECEIVE")
            self.instr.write(
                "#10",
                termination="\n",
            )
            new = self.instr.query_binary_values(
                "SEND",
                datatype="h",
                header_fmt="ieee",
                is_big_endian=True,
                expect_termination=False,
                chunk_size=6,
                container=np.array if np else list,
            )

            assert not new.size if np else not new

    def test_delay_in_query_ascii(self):
        """Test handling of the delay argument in query_ascii_values."""
        # Test using the instrument wide delay
        self.instr.query_delay = 1.0
        self.instr.write("RECEIVE")
        self.instr.write("1,2,3,4,5")
        tic = time.perf_counter()
        values = self.instr.query_ascii_values("SEND")
        assert time.perf_counter() - tic > 0.99
        assert isinstance(values[0], float)
        assert values == [1.0, 2.0, 3.0, 4.0, 5.0]

        # Test specifying the delay
        self.instr.query_delay = 0.0
        self.instr.write("RECEIVE")
        self.instr.write("1,2,3,4,5")
        tic = time.perf_counter()
        values = self.instr.query_ascii_values("SEND", delay=1.0)
        assert time.perf_counter() - tic > 0.99
        assert isinstance(values[0], float)
        assert values == [1.0, 2.0, 3.0, 4.0, 5.0]

        # Test specifying a 0 delay
        self.instr.query_delay = 1.0
        self.instr.write("RECEIVE")
        self.instr.write("1,2,3,4,5")
        tic = time.perf_counter()
        values = self.instr.query_ascii_values("SEND", delay=0.0)
        assert time.perf_counter() - tic < 0.99
        assert isinstance(values[0], float)
        assert values == [1.0, 2.0, 3.0, 4.0, 5.0]

    def test_instrument_wide_delay_in_query_binary(self):
        """Test handling delay in query_ascii_values."""
        header = "#0"
        data = [1, 2, 3328, 3, 4, 5]
        # Test using the instrument wide delay
        self.instr.query_delay = 1.0
        self.instr.write("RECEIVE")
        self.instr.write(
            header + "\x00\x01\x00\x02\r\x00\x00\x03\x00\x04\x00\x05",
            termination="\r\n",
        )
        tic = time.perf_counter()
        new = self.instr.query_binary_values(
            "SEND",
            datatype="h",
            header_fmt="ieee",
            is_big_endian=True,
            expect_termination=False,
            chunk_size=6,
            data_points=6,
        )

        assert time.perf_counter() - tic > 0.99
        assert data == new

    def test_delay_args_in_query_binary(self):
        """Test handling of the delay argument in query_ascii_values."""
        header = "#0"
        data = [1, 2, 3328, 3, 4, 5]
        self.instr.query_delay = 0.0
        self.instr.write("RECEIVE")
        self.instr.write(
            header + "\x00\x01\x00\x02\r\x00\x00\x03\x00\x04\x00\x05",
            termination="\r\n",
        )
        tic = time.perf_counter()
        new = self.instr.query_binary_values(
            "SEND",
            datatype="h",
            header_fmt="ieee",
            is_big_endian=True,
            expect_termination=False,
            chunk_size=6,
            data_points=6,
            delay=1.0,
        )

        assert time.perf_counter() - tic > 0.99
        assert data == new

    def test_no_delay_args_in_query_binary(self):
        """Test handling of the delay argument in query_ascii_values."""
        header = "#0"
        data = [1, 2, 3328, 3, 4, 5]
        self.instr.query_delay = 1.0
        self.instr.write("RECEIVE")
        self.instr.write(
            header + "\x00\x01\x00\x02\r\x00\x00\x03\x00\x04\x00\x05",
            termination="\r\n",
        )
        tic = time.perf_counter()
        new = self.instr.query_binary_values(
            "SEND",
            datatype="h",
            header_fmt="ieee",
            is_big_endian=True,
            expect_termination=False,
            chunk_size=6,
            data_points=6,
            delay=0.0,
        )

        assert time.perf_counter() - tic < 1.0
        assert data == new

    def test_stb(self):
        """Test reading the status byte."""
        assert 0 <= self.instr.stb <= 256
        assert 0 <= self.instr.read_stb() <= 256


class EventAwareMessagebasedResourceTestCaseMixin(EventAwareResourceTestCaseMixin):
    """Mixin for message based resources supporting events."""

    def test_manually_called_handlers(self):
        """Test calling manually even handler."""

        class FalseResource(Resource):
            session = None
            visalib = None
            _session = None

            def __init__(self):
                pass

        fres = FalseResource()
        fres2 = FalseResource()
        fres2.session = 1

        handler = EventHandler()
        false_wrapped_handler = fres.wrap_handler(handler.simplified_handler)
        false_wrapped_handler(None, EventType.clear, 1, 1)
        assert handler.event_success

        with pytest.raises(RuntimeError):
            false_wrapped_handler(1, EventType.clear, 1, 1)

    def test_handling_invalid_handler(self):
        """Test handling an error related to a wrong handler type."""
        with pytest.raises(errors.VisaTypeError):
            event_type = EventType.exception
            self.instr.install_handler(event_type, 1, object())

    def test_uninstalling_missing_visa_handler(self):
        """Test uninstalling a visa handler that was not registered."""
        handler1 = EventHandler()
        handler2 = EventHandler()
        event_type = EventType.exception
        self.instr.install_handler(event_type, handler1.handle_event)
        with pytest.raises(errors.UnknownHandler):
            self.instr.uninstall_handler(event_type, handler2.handle_event)

        self.instr.uninstall_handler(event_type, handler1.handle_event)

        with pytest.raises(errors.UnknownHandler):
            self.instr.uninstall_handler(event_type, handler2.handle_event)

    def test_handler_clean_up_on_resource_del(self):
        """Test that handlers are properly cleaned when a resource is deleted."""
        handler = EventHandler()
        event_type = EventType.exception
        self.instr.install_handler(event_type, handler.handle_event)

        self.instr = None
        gc.collect()
        assert not self.rm.visalib.handlers

    def test_uninstall_all_handlers(self):
        """Test uninstall all handlers from all sessions."""
        handler = EventHandler()
        event_type = EventType.exception
        self.instr.install_handler(event_type, handler.handle_event)

        self.rm.visalib.uninstall_all_visa_handlers(None)
        assert not self.rm.visalib.handlers

    def test_manual_async_read(self):
        """Test handling IOCompletion event which has extra attributes."""
        # Prepare message
        self.instr.write_raw(b"RECEIVE\n")
        self.instr.write_raw(b"test\n")
        self.instr.write_raw(b"SEND\n")

        # Enable event handling
        event_type = EventType.io_completion
        event_mech = constants.EventMechanism.queue
        wait_time = 2000  # set time that program waits to receive event
        self.instr.enable_event(event_type, event_mech, None)

        try:
            visalib = self.instr.visalib
            buffer, job_id, _status_code = visalib.read_asynchronously(
                self.instr.session, 10
            )
            assert buffer is visalib.get_buffer_from_id(job_id)
            response = self.instr.wait_on_event(event_type, wait_time)
        finally:
            self.instr.disable_event(event_type, event_mech)

        assert response.event.status == constants.StatusCode.success
        assert bytes(buffer) == bytes(response.event.buffer)
        assert bytes(response.event.data) == b"test\n"
        assert response.event.return_count == 5
        assert response.event.operation_name == "viReadAsync"

    def test_getting_unknown_buffer(self):
        """Test getting a buffer with a wrong ID."""
        assert self.instr.visalib.get_buffer_from_id(1) is None

    def test_wait_on_event_timeout(self):
        """Test waiting on a VISA event."""
        event_type = EventType.service_request
        event_mech = constants.EventMechanism.queue
        # Emit a clear to avoid dealing with previous requests
        self.instr.clear()
        self.instr.enable_event(event_type, event_mech, None)
        try:
            response = self.instr.wait_on_event(event_type, 10, capture_timeout=True)
        finally:
            self.instr.disable_event(event_type, event_mech)
        assert response.timed_out
        assert response.event.event_type == event_type

        with pytest.raises(errors.VisaIOError):
            self.instr.enable_event(event_type, event_mech, None)
            try:
                response = self.instr.wait_on_event(event_type, 10)
            finally:
                self.instr.disable_event(event_type, event_mech)

    def test_wait_on_event(self):
        """Test waiting on a VISA event."""
        event_type = EventType.service_request
        event_mech = constants.EventMechanism.queue
        wait_time = 2000  # set time that program waits to receive event
        self.instr.enable_event(event_type, event_mech, None)
        self.instr.write("RCVSLOWSRQ")
        self.instr.write("1")
        self.instr.write("SENDSLOWSRQ")
        try:
            response = self.instr.wait_on_event(event_type, wait_time)
        finally:
            self.instr.disable_event(event_type, event_mech)

        assert not response.timed_out
        assert response.event.event_type == EventType.service_request
        assert self.instr.read() == "1"

    def test_managing_visa_handler(self):
        """Test using visa handlers."""

        def _test(handle):
            handler = EventHandler()
            event_type = EventType.service_request
            event_mech = constants.EventMechanism.handler
            user_handle = self.instr.install_handler(
                event_type, handler.handle_event, user_handle=handle
            )
            self.instr.enable_event(event_type, event_mech, None)
            self.instr.write("RCVSLOWSRQ")
            self.instr.write("1")
            self.instr.write("SENDSLOWSRQ")

            try:
                t1 = time.time()
                while not handler.event_success:
                    if (time.time() - t1) > 2:
                        break
                    time.sleep(0.1)
            finally:
                self.instr.disable_event(event_type, event_mech)
                self.instr.uninstall_handler(
                    event_type, handler.handle_event, user_handle
                )

            assert handler.session == self.instr.session
            assert self.compare_user_handle(handler.handle, user_handle)
            assert handler.srq_success
            assert self.instr.read() == "1"
            self.instr.clear()

        class Point(ctypes.Structure):
            _fields_ = [("x", ctypes.c_int), ("y", ctypes.c_int)]

            def __eq__(self, other):
                if type(self) is not type(other):
                    return False
                return self.x == other.x and self.y == other.y

        for handle in (1, 1.0, "1", [1], [1.0], Point(1, 2)):
            print(handle)
            _test(handle)

    def test_wrapping_handler(self):
        """Test wrapping a handler using a Resource."""

        handler = EventHandler()
        event_type = EventType.service_request
        event_mech = constants.EventMechanism.handler
        wrapped_handler = self.instr.wrap_handler(handler.simplified_handler)
        user_handle = self.instr.install_handler(event_type, wrapped_handler, 1)
        self.instr.enable_event(event_type, event_mech, None)
        self.instr.write("RCVSLOWSRQ")
        self.instr.write("1")
        self.instr.write("SENDSLOWSRQ")

        try:
            t1 = time.time()
            while not handler.event_success:
                if (time.time() - t1) > 2:
                    break
                time.sleep(0.1)
        finally:
            self.instr.disable_event(event_type, event_mech)
            self.instr.uninstall_handler(event_type, wrapped_handler, user_handle)

        assert self.instr.session == handler.session
        assert self.compare_user_handle(handler.handle, user_handle)
        assert handler.srq_success
        assert self.instr.read() == "1"

    def test_bare_handler(self):
        """Test using a bare handler passing raw backend values."""
        from pyvisa import ctwrapper

        if not isinstance(self.instr.visalib, ctwrapper.IVIVisaLibrary):
            return

        ctwrapper.WRAP_HANDLER = False
        try:
            handler = EventHandler()
            event_type = EventType.service_request
            event_mech = constants.EventMechanism.handler
            user_handle = self.instr.install_handler(
                event_type, handler.handle_event, 1
            )
            self.instr.enable_event(event_type, event_mech, None)
            self.instr.write("RCVSLOWSRQ")
            self.instr.write("1")
            self.instr.write("SENDSLOWSRQ")

            try:
                t1 = time.time()
                while not handler.event_success:
                    if (time.time() - t1) > 2:
                        break
                    time.sleep(0.1)
            finally:
                self.instr.disable_event(event_type, event_mech)
                self.instr.uninstall_handler(
                    event_type, handler.handle_event, user_handle
                )

            assert self.instr.session == handler.session.value
            assert self.compare_user_handle(handler.handle.contents, user_handle)
            assert handler.srq_success
            assert self.instr.read() == "1"
        finally:
            ctwrapper.WRAP_HANDLER = True


class LockableMessagedBasedResourceTestCaseMixin(LockableResourceTestCaseMixin):
    """Mixing for message based resources supporting locking."""

    def test_shared_locking(self):
        """Test locking/unlocking a resource."""
        instr2 = self.rm.open_resource(str(self.rname))
        instr3 = self.rm.open_resource(str(self.rname))

        key = self.instr.lock()
        instr2.lock(requested_key=key)

        assert self.instr.query("*IDN?")
        assert instr2.query("*IDN?")
        with pytest.raises(errors.VisaIOError):
            instr3.query("*IDN?")

        # Share the lock for a limited time
        with instr3.lock_context(requested_key=key) as key2:
            assert instr3.query("*IDN?")
            assert key == key2

        # Stop sharing the lock
        instr2.unlock()

        with pytest.raises(errors.VisaIOError):
            instr2.query("*IDN?")
        with pytest.raises(errors.VisaIOError):
            instr3.query("*IDN?")

        self.instr.unlock()

        assert instr3.query("*IDN?")

    def test_exclusive_locking(self):
        """Test locking/unlocking a resource."""
        instr2 = self.rm.open_resource(str(self.rname))

        self.instr.lock_excl()
        with pytest.raises(errors.VisaIOError):
            instr2.query("*IDN?")

        self.instr.unlock()

        assert instr2.query("*IDN?")

        # Share the lock for a limited time
        with self.instr.lock_context(requested_key="exclusive") as key:
            assert key is None
            with pytest.raises(errors.VisaIOError):
                instr2.query("*IDN?")
