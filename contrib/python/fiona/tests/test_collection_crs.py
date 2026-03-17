import os
import re

import pytest

import fiona
import fiona.crs
from fiona.errors import CRSError
from .conftest import WGS84PATTERN


def test_collection_crs_wkt(path_coutwildrnp_shp):
    with fiona.open(path_coutwildrnp_shp) as src:
        assert re.match(WGS84PATTERN, src.crs_wkt)


def test_collection_no_crs_wkt(tmpdir, path_coutwildrnp_shp):
    """crs members of a dataset with no crs can be accessed safely."""
    filename = str(tmpdir.join("test.shp"))
    with fiona.open(path_coutwildrnp_shp) as src:
        profile = src.meta
    del profile['crs']
    del profile['crs_wkt']
    with fiona.open(filename, 'w', **profile) as dst:
        assert dst.crs_wkt == ""
        assert dst.crs == fiona.crs.CRS()


def test_collection_create_crs_wkt(tmpdir):
    """A collection can be created using crs_wkt"""
    filename = str(tmpdir.join("test.geojson"))
    wkt = 'GEOGCS["GCS_WGS_1984",DATUM["WGS_1984",SPHEROID["WGS_84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295],AUTHORITY["EPSG","4326"]]'
    with fiona.open(filename, 'w', schema={'geometry': 'Point', 'properties': {'foo': 'int'}}, crs_wkt=wkt, driver='GeoJSON') as dst:
        assert dst.crs_wkt.startswith('GEOGCS["WGS 84') or dst.crs_wkt.startswith('GEOGCS["GCS_WGS_1984')

    with fiona.open(filename) as col:
        assert col.crs_wkt.startswith('GEOGCS["WGS 84') or col.crs_wkt.startswith('GEOGCS["GCS_WGS_1984')


def test_collection_urn_crs(tmpdir):
    filename = str(tmpdir.join("test.geojson"))
    crs = "urn:ogc:def:crs:OGC:1.3:CRS84"
    with fiona.open(filename, 'w', schema={'geometry': 'Point', 'properties': {'foo': 'int'}}, crs=crs, driver='GeoJSON') as dst:
        assert dst.crs_wkt.startswith('GEOGCS["WGS 84')

    with fiona.open(filename) as col:
        assert col.crs_wkt.startswith('GEOGCS["WGS 84')



def test_collection_invalid_crs(tmpdir):
    filename = str(tmpdir.join("test.geojson"))
    with pytest.raises(CRSError):
        with fiona.open(filename, 'w', schema={'geometry': 'Point', 'properties': {'foo': 'int'}}, crs="12ab-invalid", driver='GeoJSON') as dst:
            pass

def test_collection_invalid_crs_wkt(tmpdir):
    filename = str(tmpdir.join("test.geojson"))
    with pytest.raises(CRSError):
        with fiona.open(filename, 'w', schema={'geometry': 'Point', 'properties': {'foo': 'int'}}, crs_wkt="12ab-invalid", driver='GeoJSON') as dst:
            pass
