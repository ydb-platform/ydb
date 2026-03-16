# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the Rotated Geodetic coordinate system.

"""

import cartopy.crs as ccrs
from .helpers import check_proj_params


common_other_args = {'o_proj=latlon', 'to_meter=111319.4907932736'}


def test_default():
    geos = ccrs.RotatedGeodetic(30, 15, 27)
    other_args = {'datum=WGS84', 'ellps=WGS84', 'lon_0=210', 'o_lat_p=15',
                  'o_lon_p=27'} | common_other_args
    check_proj_params('ob_tran', geos, other_args)
