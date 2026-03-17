# -*- coding: utf-8 -*-
"""Common test case for all message based resources."""

import logging

from pyvisa import ResourceManager, rname
from pyvisa.resources import Resource

from .. import BaseTestCase
from . import require_virtual_instr


@require_virtual_instr
class TestFilter2(BaseTestCase):
    def setup_method(self):
        """Create a ResourceManager with the default backend library."""
        self.rm = ResourceManager()

    def teardown_method(self):
        """Close the ResourceManager."""
        self.rm.close()

    def _test_filter2(self, expr, *correct):
        resources = self.rm.list_resources(expr.split("{")[0])
        ok = tuple(resources[n] for n in correct)
        filtered = rname.filter2(
            resources, expr, lambda rsc: self.rm.open_resource(rsc)
        )
        assert filtered == ok

    def test_filter2_optional_clause_with_connection(self, caplog, monkeypatch):
        self._test_filter2(
            "?*::INSTR{VI_ATTR_TERMCHAR_EN == 1 && VI_ATTR_TERMCHAR == 0}"
        )
        # Linefeed \n is 10
        self._test_filter2("TCPIP::?*::INSTR{VI_ATTR_TERMCHAR == 10}")

        # test handling error in the evaluation of the attribute
        def broken_get_visa_attribute(self, name):
            raise Exception()

        monkeypatch.setattr(Resource, "get_visa_attribute", broken_get_visa_attribute)

        # Using any other log level will cause the test to fail for no apparent
        # good reason
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger="pyvisa"):
            self._test_filter2("TCPIP::?*::INSTR{VI_ATTR_TERMCHAR == 10}")

        assert caplog.records
