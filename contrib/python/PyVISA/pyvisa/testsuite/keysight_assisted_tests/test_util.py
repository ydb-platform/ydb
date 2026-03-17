# -*- coding: utf-8 -*-
"""Test pyvisa utility functions that requires a VISA library to be tested."""

from pyvisa import util
from pyvisa.testsuite import BaseTestCase


class TestSystemDetailsAnalysis(BaseTestCase):
    """Test getting details about the VISA library architecture."""

    def test_getting_system_details_with_backend(self):
        """Test getting the system details with the backend details."""
        details = util.get_system_details(True)
        assert details["backends"]
