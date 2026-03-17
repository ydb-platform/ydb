"""Tests of the transform submodule"""

import math

import pytest

import fiona
from fiona import transform
from fiona.errors import FionaDeprecationWarning, TransformError
from fiona.model import Geometry

from .conftest import requires_gdal_lt_3


TEST_GEOMS = [
    Geometry(type="Point", coordinates=[0.0, 0.0, 1000.0]),
    Geometry(type="LineString", coordinates=[[0.0, 0.0, 1000.0], [0.1, 0.1, -1000.0]]),
    Geometry(type="MultiPoint", coordinates=[[0.0, 0.0, 1000.0], [0.1, 0.1, -1000.0]]),
    Geometry(
        type="Polygon",
        coordinates=[
            [
                [0.0, 0.0, 1000.0],
                [0.1, 0.1, -1000.0],
                [0.1, -0.1, math.pi],
                [0.0, 0.0, 1000.0],
            ]
        ],
    ),
    Geometry(
        type="MultiPolygon",
        coordinates=[
            [
                [
                    [0.0, 0.0, 1000.0],
                    [0.1, 0.1, -1000.0],
                    [0.1, -0.1, math.pi],
                    [0.0, 0.0, 1000.0],
                ]
            ]
        ],
    ),
]


@pytest.mark.parametrize("geom", TEST_GEOMS)
def test_transform_geom_with_z(geom):
    """Transforming a geom with Z succeeds"""
    transform.transform_geom("epsg:4326", "epsg:3857", geom)


@pytest.mark.parametrize("geom", TEST_GEOMS)
def test_transform_geom_array_z(geom):
    """Transforming a geom array with Z succeeds"""
    g2 = transform.transform_geom(
        "epsg:4326",
        "epsg:3857",
        [geom for _ in range(5)],
    )
    assert isinstance(g2, list)
    assert len(g2) == 5


@pytest.mark.parametrize(
    "crs",
    [
        "epsg:4326",
        "EPSG:4326",
        "WGS84",
        {"init": "epsg:4326"},
        {"proj": "longlat", "datum": "WGS84", "no_defs": True},
        "OGC:CRS84",
    ],
)
def test_axis_ordering_rev(crs):
    """Test if transform uses traditional_axis_mapping"""
    expected = (-8427998.647958742, 4587905.27136252)
    t1 = transform.transform(crs, "epsg:3857", [-75.71], [38.06])
    assert (t1[0][0], t1[1][0]) == pytest.approx(expected)
    geom = Geometry.from_dict(**{"type": "Point", "coordinates": [-75.71, 38.06]})
    g1 = transform.transform_geom(crs, "epsg:3857", geom)
    assert g1["coordinates"] == pytest.approx(expected)


@pytest.mark.parametrize(
    "crs",
    [
        "epsg:4326",
        "EPSG:4326",
        "WGS84",
        {"init": "epsg:4326"},
        {"proj": "longlat", "datum": "WGS84", "no_defs": True},
        "OGC:CRS84",
    ],
)
def test_axis_ordering_fwd(crs):
    """Test if transform uses traditional_axis_mapping"""
    rev_expected = (-75.71, 38.06)
    t2 = transform.transform("epsg:3857", crs, [-8427998.647958742], [4587905.27136252])
    assert (t2[0][0], t2[1][0]) == pytest.approx(rev_expected)
    geom = Geometry.from_dict(
        **{"type": "Point", "coordinates": [-8427998.647958742, 4587905.27136252]}
    )
    g2 = transform.transform_geom("epsg:3857", crs, geom)
    assert g2.coordinates == pytest.approx(rev_expected)


def test_transform_issue971():
    """See https://github.com/Toblerity/Fiona/issues/971"""
    source_crs = "EPSG:25832"
    dest_src = "EPSG:4326"
    geom = {
        "type": "GeometryCollection",
        "geometries": [
            {
                "type": "LineString",
                "coordinates": [
                    (512381.8870945257, 5866313.311218272),
                    (512371.23869999964, 5866322.282500001),
                    (512364.6014999999, 5866328.260199999),
                ],
            }
        ],
    }
    geom_transformed = transform.transform_geom(source_crs, dest_src, geom)
    assert geom_transformed.geometries[0].coordinates[0] == pytest.approx(
        (9.18427, 52.94630)
    )


def test_transform_geom_precision_deprecation():
    """Get a precision deprecation warning in 1.9."""
    with pytest.warns(FionaDeprecationWarning):
        transform.transform_geom(
            "epsg:4326",
            "epsg:3857",
            Geometry(type="Point", coordinates=(0, 0)),
            precision=2,
        )


def test_partial_reprojection_error():
    """Raise an error about full reprojection failure unless we opt in."""
    geom = {
        "type": "Polygon",
        "coordinates": (
            (
                (6453888.0, -6453888.0),
                (6453888.0, 6453888.0),
                (-6453888.0, 6453888.0),
                (-6453888.0, -6453888.0),
                (6453888.0, -6453888.0),
            ),
        ),
    }
    src_crs = 'PROJCS["unknown",GEOGCS["unknown",DATUM["Unknown based on WGS84 ellipsoid",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]]],PROJECTION["Orthographic"],PARAMETER["latitude_of_origin",-90],PARAMETER["central_meridian",0],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH]]'
    dst_crs = "EPSG:4326"
    with pytest.raises(TransformError):
        _ = transform.transform_geom(src_crs, dst_crs, geom)


def test_partial_reprojection_opt_in():
    """Get no exception if we opt in to partial reprojection."""
    geom = {
        "type": "Polygon",
        "coordinates": (
            (
                (6453888.0, -6453888.0),
                (6453888.0, 6453888.0),
                (-6453888.0, 6453888.0),
                (-6453888.0, -6453888.0),
                (6453888.0, -6453888.0),
            ),
        ),
    }
    src_crs = 'PROJCS["unknown",GEOGCS["unknown",DATUM["Unknown based on WGS84 ellipsoid",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]]],PROJECTION["Orthographic"],PARAMETER["latitude_of_origin",-90],PARAMETER["central_meridian",0],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH]]'
    dst_crs = "EPSG:4326"
    with fiona.Env(OGR_ENABLE_PARTIAL_REPROJECTION=True):
        _ = transform.transform_geom(src_crs, dst_crs, geom)
