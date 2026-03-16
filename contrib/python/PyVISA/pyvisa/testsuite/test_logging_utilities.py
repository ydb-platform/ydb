# -*- coding: utf-8 -*-
"""Test pyvisa utility functions that requires a VISA library to be tested."""

import logging
from io import StringIO

import pyvisa
from pyvisa.testsuite import BaseTestCase


class TestLoggingUtilities(BaseTestCase):
    """Test standard logging utilities for pyvisa."""

    def teardown_method(self):
        for h in pyvisa.logger.handlers:
            if not isinstance(h, logging.NullHandler):
                pyvisa.logger.removeHandler(h)

    def test_log_to_stream(self):
        """Test redirecting the log to a custom stream."""
        stream = StringIO()
        debug_stream = StringIO()
        pyvisa.log_to_stream(stream, level=logging.INFO)
        pyvisa.log_to_stream(debug_stream)
        pyvisa.logger.debug("debug test")
        pyvisa.logger.info("info test")

        assert "debug test" in debug_stream.getvalue()
        assert "debug test" not in stream.getvalue()
        assert "info test" in debug_stream.getvalue()
        assert "info test" in stream.getvalue()

    def test_log_to_screen(self):
        """Test redirecting the log to stderr.

        Capture of stderr seems unreliable so avoid it

        """
        self._log_to_screen_helper(logging.DEBUG)

    def test_log_to_screen_custom_level(self):
        """Test redirecting the log to stdout."""
        self._log_to_screen_helper(logging.INFO)

    def _log_to_screen_helper(self, level):
        class Faker:
            def __init__(self):
                self.args = None

            def log_to_stream(self, *args):
                self.args = args

        faker = Faker()
        try:
            old = pyvisa.log_to_stream
            pyvisa.log_to_stream = faker.log_to_stream
            if level != logging.DEBUG:
                pyvisa.log_to_screen(level)
            else:
                pyvisa.log_to_screen()
        finally:
            pyvisa.log_to_stream = old
        assert faker.args == (None, level)
