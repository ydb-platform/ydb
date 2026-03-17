# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
import pytest


_HAS_PYKDTREE = True
_HAS_SCIPY = True

try:
    import pykdtree  # noqa: F401
except ImportError:
    _HAS_PYKDTREE = False

try:
    import scipy  # noqa: F401
except ImportError:
    _HAS_SCIPY = False

requires_scipy = pytest.mark.skipif(
    not _HAS_SCIPY,
    reason="scipy is required")
requires_pykdtree = pytest.mark.skipif(
    not _HAS_PYKDTREE,
    reason="pykdtree is required")
_HAS_PYKDTREE_OR_SCIPY = _HAS_PYKDTREE or _HAS_SCIPY


def pytest_configure(config):
    # Register additional markers.
    config.addinivalue_line('markers',
                            'natural_earth: mark tests that use Natural Earth '
                            'data, and the network, if not cached.')
    config.addinivalue_line('markers',
                            'network: mark tests that use the network.')
