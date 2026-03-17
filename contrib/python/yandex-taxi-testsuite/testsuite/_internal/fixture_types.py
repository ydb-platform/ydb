"""
Fixture type shortcuts for use within test fixture arguments
annotations.

.. code-block:: python

   def test_foo(foo: FooFixture):
       ...
"""

# pylint: disable=import-only-modules
# flake8: noqa
from testsuite.plugins.common import (
    LoadBinaryFixture,
    LoadFixture,
    LoadJsonFixture,
    LoadYamlFixture,
    OpenFileFixture,
    SearchPathFixture,
)
from testsuite.mockserver.classes import (
    MockserverInfoFixture,
    MockserverRequest,
    MockserverSslInfoFixture,
)
from testsuite.mockserver.server import MockserverFixture, MockserverSslFixture
from testsuite.plugins.testpoint import TestpointFixture
