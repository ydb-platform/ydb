# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the Oblique Mercator projection.

"""

from copy import deepcopy
from typing import Dict, List, NamedTuple, Tuple

import numpy as np
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


@pytest.fixture
def oblique_mercator() -> ccrs.ObliqueMercator:
    return ccrs.ObliqueMercator()


@pytest.fixture
def rotated_mercator() -> ccrs.ObliqueMercator:
    return ccrs.ObliqueMercator(azimuth=90.0)


@pytest.fixture
def plate_carree() -> ccrs.PlateCarree:
    return ccrs.PlateCarree()


class TestCrsArgs:
    point_a_plate_carree = (-3.474083, 50.727301)
    point_b_plate_carree = (0.5, 50.5)
    proj_kwargs_default = dict(
        ellps="WGS84",
        lonc="0.0",
        lat_0="0.0",
        k="1.0",
        x_0="0.0",
        y_0="0.0",
        alpha="0.0",
        units="m",
    )

    class ParamTuple(NamedTuple):
        id: str
        crs_kwargs: dict
        proj_kwargs: Dict[str, str]
        expected_a: Tuple[float, float]
        expected_b: Tuple[float, float]

    param_list: List[ParamTuple] = [
        ParamTuple(
            "default",
            dict(),
            dict(),
            (-245106.75804, 5626768.52447),
            (35451.51708, 5595849.69689),
        ),
        ParamTuple(
            "azimuth",
            dict(azimuth=90.0),
            dict(alpha="89.999"),
            (-386712.17018, 6540102.97351),
            (55680.57266, 6500330.56121),
        ),
        ParamTuple(
            "central_longitude",
            dict(central_longitude=90.0),
            dict(lonc="90.0"),
            (-4739202.85619, 10329047.01897),
            (-4786583.034, 9966930.01085),
        ),
        ParamTuple(
            "central_latitude",
            dict(central_latitude=45.0),
            dict(lat_0="45.0"),
            (-245269.04118, 642564.31415),
            (35474.56405, 611638.73957),
        ),
        ParamTuple(
            "false_easting_northing",
            dict(false_easting=1000000, false_northing=-2000000),
            dict(x_0="1000000", y_0="-2000000"),
            (754893.24196, 3626768.52447),
            (1035451.51708, 3595849.69689),
        ),
        ParamTuple(
            "scale_factor",
            # Number inherited from test_mercator.py .
            dict(scale_factor=0.939692620786),
            dict(k="0.939692620786"),
            (-230325.01183, 5287432.86131),
            (33313.52899, 5258378.6672),
        ),
        ParamTuple(
            "globe",
            dict(globe=ccrs.Globe(ellipse="sphere")),
            dict(ellps="sphere"),
            (-244502.86059, 5646357.44304),
            (35364.23322, 5615460.21872),
        ),
        ParamTuple(
            "combo",
            dict(
                azimuth=90.0,
                central_longitude=90.0,
                central_latitude=45.0,
                false_easting=1000000,
                false_northing=-2000000,
                scale_factor=0.939692620786,
                globe=ccrs.Globe(ellipse="sphere"),
            ),
            dict(
                alpha="89.999",
                lonc="90.0",
                lat_0="45.0",
                x_0="1000000",
                y_0="-2000000",
                k="0.939692620786",
                ellps="sphere",
            ),
            (-4279982.08123, 1916861.68937),
            (-4138080.80706, 1631302.04295),
        ),
    ]
    param_ids: List[str] = [p.id for p in param_list]

    @pytest.fixture(autouse=True, params=param_list, ids=param_ids)
    def make_variant_inputs(self, request) -> None:
        inputs: TestCrsArgs.ParamTuple = request.param

        self.oblique_mercator = ccrs.ObliqueMercator(**inputs.crs_kwargs)
        proj_kwargs_expected = dict(
            self.proj_kwargs_default, **inputs.proj_kwargs
        )
        self.proj_params = {
            f"{k}={v}" for k, v in proj_kwargs_expected.items()
        }

        self.expected_a = inputs.expected_a
        self.expected_b = inputs.expected_b

    def test_proj_params(self):
        check_proj_params("omerc", self.oblique_mercator, self.proj_params)

    def test_transform_point(self, plate_carree):
        # (Point equivalence has been confirmed via plotting).
        src_expected = (
            (self.point_a_plate_carree, self.expected_a),
            (self.point_b_plate_carree, self.expected_b),
        )
        for src, expected in src_expected:
            res = self.oblique_mercator.transform_point(
                *src,
                src_crs=plate_carree,
            )
            np.testing.assert_array_almost_equal(res, expected, decimal=4)


@pytest.fixture
def oblique_variants(
    oblique_mercator,
    rotated_mercator,
) -> Tuple[ccrs.ObliqueMercator, ccrs.ObliqueMercator, ccrs.ObliqueMercator]:
    """Setup three ObliqueMercator objects, two identical, for eq testing."""
    default = oblique_mercator
    alt_1 = rotated_mercator
    alt_2 = deepcopy(rotated_mercator)
    return default, alt_1, alt_2


def test_equality(oblique_variants):
    """Check == and != operators of ccrs.ObliqueMercator."""
    default, alt_1, alt_2 = oblique_variants
    assert alt_1 == alt_2
    assert alt_1 != default
    assert hash(alt_1) != hash(default)
    assert hash(alt_1) == hash(alt_2)


@pytest.mark.parametrize(
    "reverse_coord", [False, True], ids=["xy_order", "yx_order"]
)
def test_nan(oblique_mercator, plate_carree, reverse_coord):
    coord = (0.0, np.nan)
    if reverse_coord:
        coord = tuple(reversed(coord))
    res = oblique_mercator.transform_point(*coord, src_crs=plate_carree)
    assert np.all(np.isnan(res))
