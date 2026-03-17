# -*- coding: utf-8 -*-
"""PyVISA testsuite."""

import logging
from logging.handlers import BufferingHandler

import pytest

from pyvisa import ResourceManager, logger

try:
    ResourceManager()
except ValueError:
    VISA_PRESENT = False
else:
    VISA_PRESENT = True

require_visa_lib = pytest.mark.skipif(
    not VISA_PRESENT,
    reason="Requires an installed VISA library. Run on PyVISA buildbot.",
)


class TestHandler(BufferingHandler):
    def __init__(self, only_warnings=False):
        # BufferingHandler takes a "capacity" argument
        # so as to know when to flush. As we're overriding
        # shouldFlush anyway, we can set a capacity of zero.
        # You can call flush() manually to clear out the
        # buffer.
        self.only_warnings = only_warnings
        BufferingHandler.__init__(self, 0)

    def shouldFlush(self, record):
        return False

    def emit(self, record):
        if self.only_warnings and record.level != logging.WARNING:
            return
        self.buffer.append(record.__dict__)


class BaseTestCase:
    CHECK_NO_WARNING = True

    def setup_method(self):
        self._test_handler = None
        if self.CHECK_NO_WARNING:
            self._test_handler = th = TestHandler()
            th.setLevel(logging.WARNING)
            logger.addHandler(th)

    def teardown_method(self):
        if self._test_handler is not None:
            buf = self._test_handler.buffer
            length = len(buf)
            msg = "\n".join(record.get("msg", str(record)) for record in buf)
            assert length == 0, "%d warnings raised.\n%s" % (length, msg)
