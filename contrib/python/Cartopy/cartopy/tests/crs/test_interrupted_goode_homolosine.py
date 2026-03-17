# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the InterruptedGoodeHomolosine coordinate system.

"""

import numpy as np
from numpy.testing import assert_allclose
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


@pytest.mark.parametrize("emphasis", ["land", "ocean"])
def test_default(emphasis):
    igh = ccrs.InterruptedGoodeHomolosine(emphasis=emphasis)
    other_args = {"ellps=WGS84", "lon_0=0"}
    if emphasis == "land":
        check_proj_params("igh", igh, other_args)
    elif emphasis == "ocean":
        check_proj_params("igh_o", igh, other_args)
    assert_allclose(
        np.array(igh.x_limits), [-20037508.3427892, 20037508.3427892]
    )
    assert_allclose(
        np.array(igh.y_limits), [-8683259.7164347, 8683259.7164347]
    )


@pytest.mark.parametrize("emphasis", ["land", "ocean"])
def test_eccentric_globe(emphasis):
    globe = ccrs.Globe(semimajor_axis=1000, semiminor_axis=500, ellipse=None)
    igh = ccrs.InterruptedGoodeHomolosine(globe=globe, emphasis=emphasis)
    other_args = {"a=1000", "b=500", "lon_0=0"}
    if emphasis == "land":
        check_proj_params("igh", igh, other_args)
    elif emphasis == "ocean":
        check_proj_params("igh_o", igh, other_args)

    assert_allclose(np.array(igh.x_limits), [-3141.5926536, 3141.5926536])
    assert_allclose(np.array(igh.y_limits), [-1361.410035, 1361.410035])


@pytest.mark.parametrize(
    ("emphasis", "lon"),
    [("land", -10.0), ("land", 10.0), ("ocean", -10.0), ("ocean", 10.0)],
)
def test_central_longitude(emphasis, lon):
    igh = ccrs.InterruptedGoodeHomolosine(
        central_longitude=lon, emphasis=emphasis
    )
    other_args = {"ellps=WGS84", f"lon_0={lon}"}
    if emphasis == "land":
        check_proj_params("igh", igh, other_args)
    elif emphasis == "ocean":
        check_proj_params("igh_o", igh, other_args)

    assert_allclose(
        np.array(igh.x_limits),
        [-20037508.3427892, 20037508.3427892],
    )
    assert_allclose(
        np.array(igh.y_limits), [-8683259.7164347, 8683259.7164347]
    )
