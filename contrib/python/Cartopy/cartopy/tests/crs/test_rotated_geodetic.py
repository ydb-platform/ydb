# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the Transverse Mercator projection, including OSGB and OSNI.

"""

import cartopy.crs as ccrs
from .helpers import check_proj_params


common_other_args = {'o_proj=latlon', 'to_meter=111319.4907932736',
                     'a=6378137.0'}


def test_default():
    geos = ccrs.RotatedPole(60, 50, 80)
    other_args = common_other_args | {'ellps=WGS84', 'lon_0=240', 'o_lat_p=50',
                                      'o_lon_p=80'}
    check_proj_params('ob_tran', geos, other_args)
