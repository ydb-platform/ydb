"""Test loading resources.


:copyright: 2014-2024 by PyVISA-py Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import ctypes

from pyvisa.constants import InterfaceType
from pyvisa.testsuite import BaseTestCase
from pyvisa_py.sessions import Session


class TestSessions(BaseTestCase):
    """Test generic property of PyVisaLibrary."""

    def test_sessions(self):
        available = [d for d, _ in Session.iter_valid_session_classes()]
        missing = [d for d, _ in Session.iter_session_classes_issues()]

        expected = [
            (InterfaceType.tcpip, "INSTR"),
            (InterfaceType.tcpip, "SOCKET"),
            (InterfaceType.prlgx_tcpip, "INTFC"),
            (InterfaceType.gpib, "INSTR"),
        ]
        exp_missing = []
        usbs = [(InterfaceType.usb, "INSTR"), (InterfaceType.usb, "RAW")]
        try:
            import usb

            _ = usb.core.find()

            expected.extend(usbs)
        except Exception:
            exp_missing.extend(usbs)

        gpibs = [(InterfaceType.gpib, "INTFC")]
        try:
            try:
                from gpib_ctypes import gpib
                from gpib_ctypes.Gpib import Gpib
                from gpib_ctypes.gpib.gpib import _lib as gpib_lib
            except ImportError:
                import gpib  # noqa
                from Gpib import Gpib  # noqa
            else:
                # Add some extra binding not available by default
                extra_funcs = [
                    ("ibcac", [ctypes.c_int, ctypes.c_int], ctypes.c_int),
                    ("ibgts", [ctypes.c_int, ctypes.c_int], ctypes.c_int),
                    ("ibpct", [ctypes.c_int], ctypes.c_int),
                ]
                for name, argtypes, restype in extra_funcs:
                    libfunction = gpib_lib[name]
                    libfunction.argtypes = argtypes
                    libfunction.restype = restype

            expected.extend(gpibs)
        except Exception:
            exp_missing.extend(gpibs)

        asrl = (InterfaceType.asrl, "INSTR")
        prlgx_aslr = (InterfaceType.prlgx_asrl, "INTFC")
        try:
            import serial  # noqa

            expected.append(asrl)
            expected.append(prlgx_aslr)
        except Exception:
            exp_missing.append(asrl)
            exp_missing.append(prlgx_aslr)

        vicp = (InterfaceType.vicp, "INSTR")
        try:
            import pyvicp  # noqa

            expected.append(vicp)
        except Exception:
            exp_missing.append(vicp)

        print(available, missing)
        assert sorted(available) == sorted(expected)
        assert sorted(missing) == sorted(exp_missing)
