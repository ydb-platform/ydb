# -*- coding: utf-8 -*-
"""This package is meant to run against a PyVISA builbot.

The PyVISA builbot is connected to a fake instrument implemented using the
Keysight Virtual Instrument IO Test software.

For this part of the testsuite to be run, you need to set the
PYVISA_KEYSIGHT_VIRTUAL_INSTR environment value.

To enable coverage for the shell, one needs to add/edit sitecustomize.py in
site-packages and add the following::

import coverage
coverage.process_startup()

See https://coverage.readthedocs.io/en/v4.5.x/subprocess.html for details.

"""

import functools
import os
import types

import pytest

_KEYSIGHT_AVAILABLE = "PYVISA_KEYSIGHT_VIRTUAL_INSTR" in os.environ

require_virtual_instr = pytest.mark.skipif(
    not _KEYSIGHT_AVAILABLE,
    reason="Requires the Keysight virtual instrument. Run on PyVISA buildbot.",
)


# We are testing locally with only TCPIP resources on the loop back address
if _KEYSIGHT_AVAILABLE and os.environ["PYVISA_KEYSIGHT_VIRTUAL_INSTR"] == "0":
    RESOURCE_ADDRESSES = {
        "TCPIP::INSTR": "TCPIP::127.0.0.1::INSTR",
        "TCPIP::SOCKET": "TCPIP::127.0.0.1::5025::SOCKET",
    }

    ALIASES = {}

# We are running on the bot with all supported resources.
else:
    RESOURCE_ADDRESSES = {
        # "USB::INSTR": "USB::",
        "TCPIP::INSTR": "TCPIP::192.168.0.2::INSTR",
        "TCPIP::SOCKET": "TCPIP::192.168.0.2::5025::SOCKET",
        # "GPIB::INSTR": "GPIB::19::INSTR",
    }

    ALIASES = {
        "TCPIP::192.168.0.2::INSTR": "tcpip",
    }


# Even a deepcopy is not a true copy of a function.
def copy_func(f):
    """Based on http://stackoverflow.com/a/6528148/190597 (Glenn Maynard)"""
    g = types.FunctionType(
        f.__code__,
        f.__globals__,
        name=f.__name__,
        argdefs=f.__defaults__,
        closure=f.__closure__,
    )
    g = functools.update_wrapper(g, f)
    g.__kwdefaults__ = f.__kwdefaults__
    return g
