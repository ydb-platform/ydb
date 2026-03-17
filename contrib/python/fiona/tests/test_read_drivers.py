import pytest

import fiona
from fiona.errors import FionaValueError


def test_read_fail(path_coutwildrnp_shp):
    with pytest.raises(FionaValueError):
        fiona.open(path_coutwildrnp_shp, driver='GeoJSON')
    with pytest.raises(FionaValueError):
        fiona.open(path_coutwildrnp_shp, enabled_drivers=['GeoJSON'])


def test_read(path_coutwildrnp_shp):
    with fiona.open(path_coutwildrnp_shp, driver='ESRI Shapefile') as src:
        assert src.driver == 'ESRI Shapefile'
    with fiona.open(
            path_coutwildrnp_shp,
            enabled_drivers=['GeoJSON', 'ESRI Shapefile']) as src:
        assert src.driver == 'ESRI Shapefile'
