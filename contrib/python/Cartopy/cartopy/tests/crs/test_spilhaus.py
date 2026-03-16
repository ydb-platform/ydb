# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the Spilhaus projection.
"""
from packaging.version import parse as parse_version
import pyproj
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


proj_version = parse_version(pyproj.proj_version_str)
common_arg = {
    'ellps=WGS84',
    'no_defs',
}
@pytest.mark.skipif(
    (proj_version < parse_version("9.6.0")),
    reason="Requires PROJ >= 9.6.0"
)
def test_defaults():
    crs = ccrs.Spilhaus()
    expected = {'rot=45','x_0=0.0','y_0=0.0'} | common_arg
    check_proj_params('spilhaus', crs, expected)

@pytest.mark.skipif(
    (proj_version < parse_version("9.6.0")),
    reason="Requires PROJ >= 9.6.0"
)
@pytest.mark.parametrize("rotation",[45,135,225])
def test_rotation(rotation):
    crs = ccrs.Spilhaus(rotation = rotation)
    expected = {f'rot={rotation}','x_0=0.0','y_0=0.0'} | common_arg
    check_proj_params('spilhaus', crs, expected)
