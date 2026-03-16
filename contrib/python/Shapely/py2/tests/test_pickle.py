import pytest
from shapely.geometry import Point, LineString, LinearRing, Polygon, MultiPoint

import sys
if sys.version_info[0] >= 3:
    from pickle import dumps, loads, HIGHEST_PROTOCOL
else:
    from cPickle import dumps, loads, HIGHEST_PROTOCOL

TEST_DATA = {
    "point2d": (Point, [(1.0, 2.0)]),
    "point3d": (Point, [(1.0, 2.0, 3.0)]),
    "linestring": (LineString, [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0)]),
    "linearring": (LinearRing, [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]),
    "polygon": (Polygon, [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]),
    "multipoint": (MultiPoint, [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]),
}
TEST_NAMES, TEST_DATA = zip(*TEST_DATA.items())
@pytest.mark.parametrize("cls,coords", TEST_DATA, ids=TEST_NAMES)
def test_pickle_round_trip(cls, coords):
    geom1 = cls(coords)
    assert geom1.has_z == (len(coords[0]) == 3)
    data = dumps(geom1, HIGHEST_PROTOCOL)
    geom2 = loads(data)
    assert geom2.has_z == geom1.has_z
    assert type(geom2) is type(geom1)
    assert geom2.type == geom1.type
    assert geom2.wkt == geom1.wkt
