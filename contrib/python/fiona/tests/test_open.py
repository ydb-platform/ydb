"""Tests of file opening"""

import io
import os

import pytest

import fiona
from fiona.crs import CRS
from fiona.errors import DriverError
from fiona.model import Feature


def test_open_shp(path_coutwildrnp_shp):
    """Open a shapefile"""
    assert fiona.open(path_coutwildrnp_shp)


def test_open_filename_with_exclamation(data_dir):
    path = os.path.relpath(os.path.join(data_dir, "!test.geojson"))
    assert os.path.exists(path), "Missing test data"
    assert fiona.open(path), "Failed to open !test.geojson"


def test_write_memfile_crs_wkt():
    example_schema = {
        "geometry": "Point",
        "properties": [("title", "str")],
    }

    example_features = [
        Feature.from_dict(
            **{
                "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                "properties": {"title": "One"},
            }
        ),
        Feature.from_dict(
            **{
                "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
                "properties": {"title": "Two"},
            }
        ),
        Feature.from_dict(
            **{
                "geometry": {"type": "Point", "coordinates": [3.0, 4.0]},
                "properties": {"title": "Three"},
            }
        ),
    ]

    with io.BytesIO() as fd:
        with fiona.open(
            fd,
            "w",
            driver="GPKG",
            schema=example_schema,
            crs_wkt=CRS.from_epsg(32611).to_wkt(),
        ) as dst:
            dst.writerecords(example_features)

        fd.seek(0)
        with fiona.open(fd) as src:
            assert src.driver == "GPKG"
            assert src.crs == "EPSG:32611"
