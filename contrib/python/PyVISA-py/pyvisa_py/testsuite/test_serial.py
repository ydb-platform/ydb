"""Test creating a resource manager using PyVISA-Py as a backend.


:copyright: 2014-2024 by PyVISA-py Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import pytest

from pyvisa import ResourceManager
from pyvisa.testsuite import BaseTestCase


class TestSerial(BaseTestCase):
    """Test generic property of PyVisaLibrary."""

    serial = pytest.importorskip("serial", reason="PySerial not installed")

    def test_serial(self):
        """Test loop://"""
        msg = b"Test01234567890"

        available = ["loop://"]
        expected = []
        exp_missing = []
        missing = {}

        rm = ResourceManager("@py")
        try:
            dut = rm.open_resource("ASRLloop://::INSTR")
            print("opened")
            dut.timeout = 3000
            dut.read_termination = "\r\n"
            dut.write_termination = "\r\n"
            dut.write(str(msg))
            ret_val = dut.read()
            if str(msg) == ret_val:
                expected = ["loop://"]

        except Exception:
            exp_missing = ["loop://"]

        assert sorted(available) == sorted(expected)
        assert sorted(missing) == sorted(exp_missing)
