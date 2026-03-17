from shapely import geometry


def test_polygon():
    assert bool(geometry.Polygon()) is False


def test_linestring():
    assert bool(geometry.LineString()) is False


def test_point():
    assert bool(geometry.Point()) is False


def test_geometry_collection():
    assert bool(geometry.GeometryCollection()) is False
