"""Tests for geometry objects."""

import pytest

from fiona._geometry import GeomBuilder, geometryRT
from fiona.errors import UnsupportedGeometryTypeError
from fiona.model import Geometry


def geometry_wkb(wkb):
    try:
        wkb = bytes.fromhex(wkb)
    except AttributeError:
        wkb = wkb.decode("hex")
    return GeomBuilder().build_wkb(wkb)


def test_ogr_builder_exceptions():
    geom = Geometry.from_dict(**{"type": "Bogus", "coordinates": None})
    with pytest.raises(UnsupportedGeometryTypeError):
        geometryRT(geom)


@pytest.mark.parametrize(
    "geom_type, coordinates",
    [
        ("Point", (0.0, 0.0)),
        ("LineString", [(0.0, 0.0), (1.0, 1.0)]),
        ("Polygon", [[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)]]),
        ("MultiPoint", [(0.0, 0.0), (1.0, 1.0)]),
        ("MultiLineString", [[(0.0, 0.0), (1.0, 1.0)]]),
        (
            "MultiPolygon",
            [[[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)]]],
        ),
    ],
)
def test_round_tripping(geom_type, coordinates):
    result = geometryRT(
        Geometry.from_dict(**{"type": geom_type, "coordinates": coordinates})
    )
    assert result.type == geom_type
    assert result.coordinates == coordinates


@pytest.mark.parametrize(
    "geom_type, coordinates",
    [
        ("Polygon", [[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]]),
        ("MultiPolygon", [[[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]]]),
    ],
)
def test_implicitly_closed_round_tripping(geom_type, coordinates):
    result = geometryRT(
        Geometry.from_dict(**{"type": geom_type, "coordinates": coordinates})
    )
    assert result.type == geom_type
    result_coordinates = result.coordinates
    while not isinstance(coordinates[0], tuple):
        result_coordinates = result_coordinates[0]
        coordinates = coordinates[0]
    assert result_coordinates[:-1] == coordinates


def test_geometry_collection_round_trip():
    geom = {
        "type": "GeometryCollection",
        "geometries": [
            {"type": "Point", "coordinates": (0.0, 0.0)},
            {"type": "LineString", "coordinates": [(0.0, 0.0), (1.0, 1.0)]},
        ],
    }

    result = geometryRT(geom)
    assert len(result["geometries"]) == 2
    assert [g["type"] for g in result["geometries"]] == ["Point", "LineString"]


def test_point_wkb():
    # Hex-encoded Point (0 0)
    wkb = "010100000000000000000000000000000000000000"
    geom = geometry_wkb(wkb)
    assert geom["type"] == "Point"
    assert geom["coordinates"] == (0.0, 0.0)


def test_line_wkb():
    # Hex-encoded LineString (0 0, 1 1)
    wkb = (
        "01020000000200000000000000000000000000000000000000000000000000f03f"
        "000000000000f03f"
    )
    geom = geometry_wkb(wkb)
    assert geom["type"] == "LineString"
    assert geom["coordinates"] == [(0.0, 0.0), (1.0, 1.0)]


def test_polygon_wkb():
    # 1 x 1 box (0, 0, 1, 1)
    wkb = (
        "01030000000100000005000000000000000000f03f000000000000000000000000"
        "0000f03f000000000000f03f0000000000000000000000000000f03f0000000000"
        "0000000000000000000000000000000000f03f0000000000000000"
    )
    geom = geometry_wkb(wkb)
    assert geom["type"], "Polygon"
    assert len(geom["coordinates"]) == 1
    assert len(geom["coordinates"][0]) == 5
    x, y = zip(*geom["coordinates"][0])
    assert min(x) == 0.0
    assert min(y) == 0.0
    assert max(x) == 1.0
    assert max(y) == 1.0


def test_multipoint_wkb():
    wkb = (
        "010400000002000000010100000000000000000000000000000000000000010100"
        "0000000000000000f03f000000000000f03f"
    )
    geom = geometry_wkb(wkb)
    assert geom["type"] == "MultiPoint"
    assert geom["coordinates"] == [(0.0, 0.0), (1.0, 1.0)]


def test_multilinestring_wkb():
    # Hex-encoded LineString (0 0, 1 1)
    wkb = (
        "010500000001000000010200000002000000000000000000000000000000000000"
        "00000000000000f03f000000000000f03f"
    )
    geom = geometry_wkb(wkb)
    assert geom["type"] == "MultiLineString"
    assert len(geom["coordinates"]) == 1
    assert len(geom["coordinates"][0]) == 2
    assert geom["coordinates"][0] == [(0.0, 0.0), (1.0, 1.0)]


def test_multipolygon_wkb():
    # [1 x 1 box (0, 0, 1, 1)]
    wkb = (
        "01060000000100000001030000000100000005000000000000000000f03f000000"
        "0000000000000000000000f03f000000000000f03f000000000000000000000000"
        "0000f03f00000000000000000000000000000000000000000000f03f0000000000"
        "000000"
    )
    geom = geometry_wkb(wkb)
    assert geom["type"] == "MultiPolygon"
    assert len(geom["coordinates"]) == 1
    assert len(geom["coordinates"][0]) == 1
    assert len(geom["coordinates"][0][0]) == 5
    x, y = zip(*geom["coordinates"][0][0])
    assert min(x) == 0.0
    assert min(y) == 0.0
    assert max(x) == 1.0
    assert max(y) == 1.0
