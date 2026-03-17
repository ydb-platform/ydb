from shapely.geometry import Point, MultiPoint, Polygon, GeometryCollection


def test_point():
    g = Point(0, 0)
    try:
        assert hash(g)
        return False
    except TypeError:
        return True


def test_multipoint():
    g = MultiPoint([(0, 0)])
    try:
        assert hash(g)
        return False
    except TypeError:
        return True


def test_polygon():
    g = Point(0, 0).buffer(1.0)
    try:
        assert hash(g)
        return False
    except TypeError:
        return True


def test_collection():
    g = GeometryCollection([Point(0, 0)])
    try:
        assert hash(g)
        return False
    except TypeError:
        return True
