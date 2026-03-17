"""Tests of features related to GDAL RFC 64

See https://trac.osgeo.org/gdal/wiki/rfc64_triangle_polyhedralsurface_tin.
"""

import fiona
from fiona.model import Geometry


def _test_tin(geometry: Geometry) -> None:
    """Test if TIN (((0 0 0, 0 0 1, 0 1 0, 0 0 0)), ((0 0 0, 0 1 0, 1 1 0, 0 0 0)))
    is correctly converted to MultiPolygon.
    """
    assert geometry["type"] == "MultiPolygon"
    assert geometry["coordinates"] == [
        [[(0.0, 0.0, 0.0), (0.0, 0.0, 1.0), (0.0, 1.0, 0.0), (0.0, 0.0, 0.0)]],
        [[(0.0, 0.0, 0.0), (0.0, 1.0, 0.0), (1.0, 1.0, 0.0), (0.0, 0.0, 0.0)]],
    ]


def _test_triangle(geometry: Geometry) -> None:
    """Test if TRIANGLE((0 0 0,0 1 0,1 1 0,0 0 0))
    is correctly converted to MultiPolygon."""
    assert geometry["type"] == "Polygon"
    assert geometry["coordinates"] == [
        [(0.0, 0.0, 0.0), (0.0, 1.0, 0.0), (1.0, 1.0, 0.0), (0.0, 0.0, 0.0)]
    ]


def test_tin_shp(path_test_tin_shp):
    """Convert TIN to MultiPolygon"""
    with fiona.open(path_test_tin_shp) as col:
        assert col.schema["geometry"] == "Unknown"
        features = list(col)
        assert len(features) == 1
        _test_tin(features[0]["geometry"])


def test_tin_csv(path_test_tin_csv):
    """Convert TIN to MultiPolygon and Triangle to Polygon"""
    with fiona.open(path_test_tin_csv) as col:
        assert col.schema["geometry"] == "Unknown"

        feature1 = next(col)
        _test_tin(feature1["geometry"])

        feature2 = next(col)
        _test_triangle(feature2["geometry"])

        feature3 = next(col)
        assert feature3["geometry"]["type"] == "GeometryCollection"
        assert len(feature3["geometry"]["geometries"]) == 2

        _test_tin(feature3["geometry"]["geometries"][0])
        _test_triangle(feature3["geometry"]["geometries"][1])
