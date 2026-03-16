import os
import re

import fiona

from .conftest import WGS84PATTERN

def test_profile(path_coutwildrnp_shp):
    with fiona.open(path_coutwildrnp_shp) as src:
        assert re.match(WGS84PATTERN, src.crs_wkt)


def test_profile_creation_wkt(tmpdir, path_coutwildrnp_shp):
    outfilename = str(tmpdir.join("test.shp"))
    with fiona.open(path_coutwildrnp_shp) as src:
        profile = src.meta
        profile['crs'] = 'bogus'
        with fiona.open(outfilename, 'w', **profile) as dst:
            assert dst.crs == {'init': 'epsg:4326'}
            assert re.match(WGS84PATTERN, dst.crs_wkt)
