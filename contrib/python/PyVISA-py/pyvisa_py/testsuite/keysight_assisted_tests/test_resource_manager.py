# -*- coding: utf-8 -*-
"""Test the Resource manager."""

import pytest

from pyvisa.rname import ResourceName
from pyvisa.testsuite.keysight_assisted_tests import (
    RESOURCE_ADDRESSES,
    copy_func,
    require_virtual_instr,
)
from pyvisa.testsuite.keysight_assisted_tests.test_resource_manager import (
    TestResourceManager as BaseTestResourceManager,
    TestResourceParsing as BaseTestResourceParsing,
)


@require_virtual_instr
class TestPyResourceManager(BaseTestResourceManager):
    """ """

    def test_list_resource(self):
        """Test listing the available resources.
        The bot supports only TCPIP and of those resources we expect to be able
        to list only INSTR resources not SOCKET.
        """
        # Default settings
        resources = self.rm.list_resources()
        for v in (v for v in RESOURCE_ADDRESSES.values() if v.endswith("INSTR")):
            assert str(ResourceName.from_string(v)) in resources

    test_last_status = pytest.mark.xfail(
        copy_func(BaseTestResourceManager.test_last_status)
    )

    test_opening_resource_with_lock = pytest.mark.xfail(
        copy_func(BaseTestResourceManager.test_opening_resource_with_lock)
    )


@require_virtual_instr
class TestPyResourceParsing(BaseTestResourceParsing):
    """ """

    pass
