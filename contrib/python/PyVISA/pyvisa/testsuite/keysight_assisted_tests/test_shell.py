# -*- coding: utf-8 -*-
"""Test the shell."""

import os
import time
from contextlib import redirect_stdout
from io import StringIO
from subprocess import PIPE, Popen
from threading import Event, Lock, Thread

from pyvisa import constants, errors
from pyvisa.resources import Resource
from pyvisa.rname import to_canonical_name
from pyvisa.shell import VisaShell

from .. import BaseTestCase
from . import ALIASES, RESOURCE_ADDRESSES, require_virtual_instr


class SubprocessOutputPoller:
    """Continuously check the stdout of a subprocess."""

    def __init__(self, process):
        super().__init__()
        self.process = process
        self._lines = []
        self._lines_lock = Lock()
        self._last_seen = time.monotonic()
        self.data_ready = Event()
        self._polling_thread = Thread(target=self.poll_stdout)
        self._ready_thread = Thread(target=self.check_ready)

        # Start background threads
        self._polling_thread.start()
        self._ready_thread.start()

    def poll_stdout(self):
        """Continously read stdout and update the lines.

        When no new data arrive after 1s consider that the data are ready.

        """
        for line in iter(self.process.stdout.readline, b""):
            with self._lines_lock:
                self._lines.append(line.rstrip())
                self._last_seen = time.monotonic()

    def check_ready(self):
        """Check if we got complete data."""
        while True:
            time.sleep(0.05)
            if self._lines and time.monotonic() - self._last_seen > 0.5:
                self.data_ready.set()
            if not self._polling_thread.is_alive():
                break

    def get_lines(self):
        """Get the collected lines."""
        with self._lines_lock:
            lines = self._lines
            self._lines = []

        self.data_ready.clear()
        return lines

    def shutdown(self):
        """Wait for threads to die after the process is done."""
        self._polling_thread.join()
        self._ready_thread.join()


@require_virtual_instr
class TestVisaShell(BaseTestCase):
    """Test the VISA shell."""

    def setup_method(self):
        """Start the shell in a subprocess."""
        os.environ["COVERAGE_PROCESS_START"] = ".coveragerc"
        self.shell = Popen(["pyvisa-shell"], stdin=PIPE, stdout=PIPE)
        self.reader = SubprocessOutputPoller(self.shell)
        self.reader.data_ready.wait(1)
        self.reader.get_lines()

    def open_resource(self):
        lines = self.communicate(f"open {next(iter(RESOURCE_ADDRESSES.values()))}")
        assert b"has been opened." in lines[0]

    def communicate(self, msg):
        """Write a message on stdin and collect the answer."""
        self.shell.stdin.write(msg.encode("ascii") + b"\n")
        self.shell.stdin.flush()
        self.reader.data_ready.wait(1)
        return self.reader.get_lines()

    def teardown_method(self):
        if self.shell:
            self.shell.stdin.write(b"exit\n")
            self.shell.stdin.flush()
            self.shell.stdin.close()
            self.shell.terminate()
            self.shell.wait(0.1)
            self.reader.shutdown()

    def test_complete_open(self):
        """Test providing auto-completion for open."""
        shell = VisaShell()
        completions = shell.complete_open("TCPIP", 0, 0, 0)
        assert to_canonical_name(RESOURCE_ADDRESSES["TCPIP::INSTR"]) in completions

        # Test getting an alias from the completion
        completions = shell.complete_open("tcp", 0, 0, 0)
        assert "tcpip" in completions

    def test_list(self):
        """Test listing the connected resources."""
        lines = self.communicate("list")

        msg = []
        for i, rsc in enumerate(RESOURCE_ADDRESSES.values()):
            if not rsc.endswith("INSTR"):
                continue
            msg.append(f"({i:2d}) {to_canonical_name(rsc)}")
            if rsc in ALIASES:
                msg.append(f"     alias: {ALIASES[rsc]}")

        print(lines, msg)
        for m in msg:
            assert any(m.encode("ascii") in line for line in lines)

    # TODO fix argument handling to allow filtering

    def test_list_handle_error(self):
        """Test handling an error in listing resources."""
        shell = VisaShell()
        shell.resource_manager = None
        temp_stdout = StringIO()
        with redirect_stdout(temp_stdout):
            shell.do_list("")
        output = temp_stdout.getvalue()
        assert "no attribute" in output

    def test_open_no_args(self):
        """Test opening without any argument."""
        lines = self.communicate("open")
        assert b"A resource name must be specified." in lines[0]

    def test_open_by_number(self):
        """Test opening based on the index of the resource."""
        lines = self.communicate("open 0")
        assert b'Not a valid resource number. Use the command "list".' in lines[0]

        lines = self.communicate("list")
        lines = self.communicate("open 0")
        rsc = next(iter(RESOURCE_ADDRESSES.values()))
        assert f"{to_canonical_name(rsc)} has been opened.".encode("ascii") in lines[0]

        lines = self.communicate("open 0")
        assert (
            b"You can only open one resource at a time. "
            b"Please close the current one first."
        ) in lines[0]

    def test_open_by_address(self):
        """Test opening based on the resource address."""
        rsc = next(iter(RESOURCE_ADDRESSES.values()))
        lines = self.communicate(f"open {rsc}")
        assert f"{rsc} has been opened.".encode("ascii") in lines[0]

    def test_open_handle_exception(self):
        """Test handling an exception during opening."""
        lines = self.communicate('open ""')
        assert b"VI_ERROR_INV_RSRC_NAME" in lines[0]

    def test_handle_double_open(self):
        """Test handling before closing resource."""
        rsc = next(iter(RESOURCE_ADDRESSES.values()))
        lines = self.communicate(f"open {rsc}")
        lines = self.communicate(f"open {rsc}")
        assert (
            b"You can only open one resource at a time. "
            b"Please close the current one first."
        ) in lines[0]

    def test_command_on_closed_resource(self):
        """Test all the commands that cannot be run without opening a resource."""
        for cmd in ("close", "write", "read", "query", "termchar", "timeout", "attr"):
            lines = self.communicate(cmd)
            assert b'There are no resources in use. Use the command "open".' in lines[0]

    def test_close(self):
        """Test closing a resource."""
        rsc = next(iter(RESOURCE_ADDRESSES.values()))
        lines = self.communicate(f"open {rsc}")
        assert b"has been opened." in lines[0]
        lines = self.communicate("close")
        assert b"The resource has been closed." in lines[0]

        lines = self.communicate(f"open {rsc}")
        assert b"has been opened." in lines[0]

    def test_close_handle_error(self):
        """Test handling an error while closing."""
        shell = VisaShell()
        shell.current = True
        temp_stdout = StringIO()
        with redirect_stdout(temp_stdout):
            shell.do_close("")
        output = temp_stdout.getvalue()
        assert "no attribute" in output

    def test_query(self):
        """querying a value from the instrument."""
        self.open_resource()
        lines = self.communicate("query *IDN?")
        assert b"Response:" in lines[0]

    def test_query_handle_error(self):
        """Test handling an error in query."""
        shell = VisaShell()
        shell.current = True
        temp_stdout = StringIO()
        with redirect_stdout(temp_stdout):
            shell.do_query("")
        output = temp_stdout.getvalue()
        assert "no attribute" in output

    def test_read_write(self):
        """Test writing/reading values from the resource."""
        self.open_resource()
        lines = self.communicate("write *IDN?")
        lines = self.communicate("read")
        assert b"Keysight " in lines[0]

    def test_read_handle_error(self):
        """Test handling an error in read."""
        shell = VisaShell()
        shell.current = True
        temp_stdout = StringIO()
        with redirect_stdout(temp_stdout):
            shell.do_read("")
        output = temp_stdout.getvalue()
        assert "no attribute" in output

    def test_write_handle_error(self):
        """Test handling an error in write."""
        shell = VisaShell()
        shell.current = True
        temp_stdout = StringIO()
        with redirect_stdout(temp_stdout):
            shell.do_write("")
        output = temp_stdout.getvalue()
        assert "no attribute" in output

    def test_timeout_get(self):
        """Test accessing the timeout."""
        self.open_resource()
        lines = self.communicate("timeout")
        assert b"Timeout: " in lines[0]

    def test_timeout_get_handle_error(self):
        """Test handling an error in getting teh timeout."""
        shell = VisaShell()
        shell.current = True
        temp_stdout = StringIO()
        with redirect_stdout(temp_stdout):
            shell.do_timeout("")
        output = temp_stdout.getvalue()
        assert "no attribute" in output

    def test_timeout_set(self):
        """Test setting the timeout."""
        self.open_resource()
        lines = self.communicate("timeout 1000")
        assert b"Done" in lines[0]
        lines = self.communicate("timeout")
        assert b"Timeout: 1000ms" in lines[0]

    def test_timeout_set_handle_error(self):
        """Test handling an error in setting the timeout"""
        shell = VisaShell()
        shell.current = True
        temp_stdout = StringIO()
        with redirect_stdout(temp_stdout):
            shell.do_timeout("1000")
        output = temp_stdout.getvalue()
        assert "no attribute" in output

    def test_print_attr_list(self):
        """Test printing attribute list."""

        class FalseResource:
            @classmethod
            def get_visa_attribute(cls, id):
                if id == constants.VI_ATTR_TMO_VALUE:
                    raise errors.VisaIOError(constants.VI_ERROR_NSUP_ATTR)
                elif id == constants.VI_ATTR_INTF_NUM:
                    raise Exception("Long text: aaaaaaaaaaaaaaaaaaaa")
                else:
                    raise Exception("Test")

        FalseResource.visa_attributes_classes = Resource.visa_attributes_classes

        shell = VisaShell()
        shell.current = FalseResource

        temp_stdout = StringIO()
        with redirect_stdout(temp_stdout):
            shell.print_attribute_list()

        output = temp_stdout.getvalue()
        assert "Long text:..." in output

    def test_attr_no_args(self):
        """Test getting the list of attributes"""
        self.open_resource()
        lines = self.communicate("attr")
        assert b"VISA name" in lines[1]

    def test_attr_too_many_args(self):
        """Test handling wrong args to attr."""
        self.open_resource()
        lines = self.communicate("attr 1 2 3")
        assert (
            b"Invalid syntax, use `attr <name>` to get;"
            b" or `attr <name> <value>` to set" in lines[0]
        )

    def test_issue_in_getting_attr(self):
        """Test handling exception in getting an attribute."""
        shell = VisaShell()
        shell.do_open(next(iter(RESOURCE_ADDRESSES.values())))

        def broken_get_visa_attribute(self, name=""):
            raise Exception("Exception")

        # Issue on VI_
        old = Resource.get_visa_attribute
        Resource.get_visa_attribute = broken_get_visa_attribute
        try:
            temp_stdout = StringIO()
            with redirect_stdout(temp_stdout):
                try:
                    shell.do_attr("VI_ATTR_TERMCHAR")
                finally:
                    Resource.get_visa_attribute = old
            output = temp_stdout.getvalue()
            assert "Exception" in output
        finally:
            Resource.get_visa_attribute = old

        # Issue on aliased attr
        old = type(shell.current).allow_dma
        type(shell.current).allow_dma = property(broken_get_visa_attribute)
        try:
            temp_stdout = StringIO()
            with redirect_stdout(temp_stdout):
                shell.do_attr("allow_dma")
            output = temp_stdout.getvalue()
            assert "Exception" in output
        finally:
            type(shell.current).allow_dma = old

    def test_attr_get_set_by_VI_non_boolean(self):
        """Test getting/setting an attr using the VI_ name (int value)"""
        self.open_resource()
        msg = "attr VI_ATTR_TERMCHAR {}".format(ord("\r"))
        lines = self.communicate(msg)
        assert b"Done" in lines[0]

        lines = self.communicate("attr VI_ATTR_TERMCHAR")
        assert str(ord("\r")) in lines[0].decode("ascii")

    def test_attr_get_set_by_VI_boolean(self):
        """Test getting/setting an attr using the VI_ name (bool value)"""
        self.open_resource()
        for v in (False, True):
            msg = f"attr VI_ATTR_TERMCHAR_EN {v}"
            lines = self.communicate(msg)
            assert b"Done" in lines[0]

            lines = self.communicate("attr VI_ATTR_TERMCHAR_EN")
            assert str(int(v)).encode("ascii") in lines[0]

    def test_attr_get_by_VI_handle_error(self):
        """Test accessing an attr by an unknown VI name."""
        self.open_resource()
        lines = self.communicate("attr VI_test")
        assert b"no attribute" in lines[0]

    def test_attr_get_by_name(self):
        """Test accessing an attr by Python name."""
        self.open_resource()
        lines = self.communicate("attr allow_dma")
        assert b"True" in lines[0] or b"False" in lines[0]

    def test_attr_get_by_name_handle_error(self):
        """Test accessing an attr by an unknown Python name."""
        self.open_resource()
        lines = self.communicate("attr test")
        assert b"no attribute" in lines[0]

    def test_attr_set_by_VI_handle_error_unknown_attr(self):
        """Test handling issue in setting VI attr which does not exist."""
        self.open_resource()
        lines = self.communicate("attr VI_test test")
        assert b"no attribute" in lines[0]

    def test_attr_set_by_VI_handle_error_non_boolean(self):
        """Test handling issue in setting VI attr. (non boolean value)"""
        self.open_resource()
        msg = "attr VI_ATTR_TERMCHAR_EN Test"
        lines = self.communicate(msg)
        assert b"Error" in lines[0]

    def test_attr_set_by_VI_handle_error_non_interger(self):
        """Test handling issue in setting VI attr. (non integer value)"""
        self.open_resource()
        msg = "attr VI_ATTR_TERMCHAR Test"
        lines = self.communicate(msg)
        assert b"Error" in lines[0]

    def test_attr_set_by_VI_handle_error_wrong_value(self):
        """Test handling issue in setting VI attr by name. (wrong value)"""
        self.open_resource()
        msg = "attr VI_ATTR_TERMCHAR -1"
        lines = self.communicate(msg)
        assert b"VI_ERROR_NSUP_ATTR_STATE" in lines[0]

    def test_attr_set_by_name_handle_error(self):
        """Test handling attempt to set attr by name (which is not supported)."""
        self.open_resource()
        msg = "attr allow_dma Test"
        lines = self.communicate(msg)
        assert (
            b"Setting Resource Attributes by python name is not yet "
            b"supported." in lines[0]
        )

    def test_complete_attr(self):
        """Test providing auto-completion for attrs."""
        shell = VisaShell()
        shell.do_open(next(iter(RESOURCE_ADDRESSES.values())))
        completions = shell.complete_attr("VI_ATTR_TERM", 0, 0, 0)
        assert "VI_ATTR_TERMCHAR" in completions
        assert "VI_ATTR_TERMCHAR_EN" in completions

        completions = shell.complete_attr("allow_d", 0, 0, 0)
        assert "allow_dma" in completions

    def test_termchar_get_handle_error(self):
        """Test handling error when getting the termchars."""
        shell = VisaShell()
        shell.current = True
        temp_stdout = StringIO()
        with redirect_stdout(temp_stdout):
            shell.do_termchar("")
        output = temp_stdout.getvalue()
        assert "no attribute" in output

    def test_getting_termchar_absent_mapping(self):
        """Test getting a termchar that does not map to something with a representation."""
        shell = VisaShell()
        shell.do_open(next(iter(RESOURCE_ADDRESSES.values())))
        shell.current.read_termination = "X"
        shell.current.write_termination = "Z"
        temp_stdout = StringIO()
        with redirect_stdout(temp_stdout):
            shell.do_termchar("")
        output = temp_stdout.getvalue()
        assert "Termchar read: X write: Z" == output.split("\n")[0]

    def test_termchar_get_set_both_identical(self):
        """Test setting both termchars to the same value."""
        self.open_resource()
        lines = self.communicate("termchar CR")
        assert b"Done" in lines[0]

        lines = self.communicate("termchar")
        assert b"Termchar read: CR write: CR" in lines[0]

    def test_termchar_get_set_both_different(self):
        """Test setting both termchars to different values."""
        self.open_resource()
        lines = self.communicate("termchar CR NUL")
        assert b"Done" in lines[0]

        lines = self.communicate("termchar")
        assert b"Termchar read: CR write: NUL" in lines[0]

    def test_termchar_set_too_many_args(self):
        """Test handling to many termchars to termchar."""
        self.open_resource()
        lines = self.communicate("termchar 1 2 3")
        assert b"Invalid syntax" in lines[0]

    def test_termchar_set_handle_error_wrong_value(self):
        """Test handling wrong value in setting termchar."""
        self.open_resource()
        lines = self.communicate("termchar tt")
        assert b"use CR, LF, CRLF, NUL or None to set termchar" in lines[0]

    def test_termchar_set_handle_error(self):
        """Test handling an error in setting the termchars."""
        shell = VisaShell()
        shell.current = True
        temp_stdout = StringIO()
        with redirect_stdout(temp_stdout):
            shell.do_termchar("CR")
        output = temp_stdout.getvalue()
        assert "no attribute" in output

    def test_eof(self):
        """Test handling an EOF."""
        shell = VisaShell()
        assert shell.do_EOF(None)
