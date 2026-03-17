# Python module tests

import json

import pytest  # type: ignore
import shapely  # type: ignore
from shapely.geometry import LineString, MultiPoint, Point, mapping, shape  # type: ignore

from fiona.errors import ReduceError
from fiona.features import (  # type: ignore
    map_feature,
    reduce_features,
    vertex_count,
    area,
    buffer,
    collect,
    distance,
    dump,
    identity,
    length,
    unary_projectable_property_wrapper,
    unary_projectable_constructive_wrapper,
    binary_projectable_property_wrapper,
)


def test_modulate_simple():
    """Set a feature's geometry."""
    # map_feature() is a generator. list() materializes the values.
    feat = list(map_feature("Point 0 0", {"type": "Feature"}))
    assert len(feat) == 1

    feat = feat[0]
    assert "Point" == feat["type"]
    assert (0.0, 0.0) == feat["coordinates"]


def test_modulate_complex():
    """Exercise a fairly complicated pipeline."""
    bufkwd = "resolution" if shapely.__version__.startswith("1") else "quad_segs"

    with open("tests/data/trio.geojson") as src:
        collection = json.loads(src.read())

    feat = collection["features"][0]
    results = list(
        map_feature(
            f"simplify (buffer g (* 0.1 2) :projected false :{bufkwd} (- 4 3)) 0.001 :projected false :preserve_topology false",
            feat,
        )
    )
    assert 1 == len(results)

    geom = results[0]
    assert geom["type"] == "Polygon"
    assert len(geom["coordinates"][0]) == 5


@pytest.mark.parametrize(
    "obj, count",
    [
        (Point(0, 0), 1),
        (MultiPoint([(0, 0), (1, 1)]), 2),
        (Point(0, 0).buffer(10.0).difference(Point(0, 0).buffer(1.0)), 130),
    ],
)
def test_vertex_count(obj, count):
    """Check vertex counting correctness."""
    assert count == vertex_count(obj)


@pytest.mark.parametrize(
    "obj, count",
    [
        (Point(0, 0), 1),
        (MultiPoint([(0, 0), (1, 1)]), 2),
        (Point(0, 0).buffer(10.0).difference(Point(0, 0).buffer(1.0)), 130),
    ],
)
def test_calculate_vertex_count(obj, count):
    """Confirm vertex counting is in func_map."""
    feat = {"type": "Feature", "properties": {}, "geometry": mapping(obj)}
    assert count == list(map_feature("vertex_count g", feat))[0]


def test_calculate_builtin():
    """Confirm builtin function evaluation."""
    assert 42 == list(map_feature("int '42'", None))[0]


def test_calculate_feature_attr():
    """Confirm feature attr evaluation."""
    assert "LOLWUT" == list(map_feature("upper f", "lolwut"))[0]


def test_calculate_point():
    """Confirm feature attr evaluation."""
    result = list(map_feature("Point 0 0", None))[0]
    assert "Point" == result["type"]


def test_calculate_points():
    """Confirm feature attr evaluation."""
    result = list(map_feature("list (Point 0 0) (buffer (Point 1 1) 1)", None))
    assert 2 == len(result)
    assert "Point" == result[0]["type"]
    assert "Polygon" == result[1]["type"]


def test_reduce_len():
    """Reduce can count the number of input features."""
    with open("tests/data/trio.seq") as seq:
        data = [json.loads(line) for line in seq.readlines()]

    # reduce() is a generator. list() materializes the values.
    assert 3 == list(reduce_features("len c", data))[0]


def test_reduce_union():
    """Reduce yields one feature by default."""
    with open("tests/data/trio.seq") as seq:
        data = [json.loads(line) for line in seq.readlines()]

    # reduce() is a generator. list() materializes the values.
    result = list(reduce_features("unary_union c", data))
    assert len(result) == 1

    val = result[0]
    assert "GeometryCollection" == val["type"]
    assert 2 == len(val["geometries"])


def test_reduce_union_area():
    """Reduce can yield total area using raw output."""
    with open("tests/data/trio.seq") as seq:
        data = [json.loads(line) for line in seq.readlines()]

    # reduce() is a generator.
    result = list(reduce_features("area (unary_union c)", data))
    assert len(result) == 1

    val = result[0]
    assert isinstance(val, float)
    assert 3e4 < val < 4e4


def test_reduce_union_geom_type():
    """Reduce and print geom_type using raw output."""
    with open("tests/data/trio.seq") as seq:
        data = [json.loads(line) for line in seq.readlines()]

    # reduce() is a generator.
    result = list(reduce_features("geom_type (unary_union c)", data))
    assert len(result) == 1
    assert "GeometryCollection" == result[0]


def test_reduce_error():
    """Raise ReduceError when expression doesn't reduce."""
    with open("tests/data/trio.seq") as seq:
        data = [json.loads(line) for line in seq.readlines()]

    with pytest.raises(ReduceError):
        list(reduce_features("(identity c)", data))


@pytest.mark.parametrize(
    "obj, count",
    [
        (MultiPoint([(0, 0), (1, 1)]), 2),
    ],
)
def test_dump_eval(obj, count):
    feature = {"type": "Feature", "properties": {}, "geometry": mapping(obj)}
    result = map_feature("identity g", feature, dump_parts=True)
    assert len(list(result)) == count


def test_collect():
    """Collect two points."""
    geom = collect((Point(0, 0), Point(1, 1)))
    assert geom.geom_type == "GeometryCollection"


def test_dump():
    """Dump a point."""
    geoms = list(dump(Point(0, 0)))
    assert len(geoms) == 1
    assert geoms[0].geom_type == "Point"


def test_dump_multi():
    """Dump two points."""
    geoms = list(dump(MultiPoint([(0, 0), (1, 1)])))
    assert len(geoms) == 2
    assert all(g.geom_type == "Point" for g in geoms)


def test_identity():
    """Check identity."""
    geom = Point(1.1, 2.2)
    assert geom == identity(geom)


def test_area():
    """Check projected area of RMNP against QGIS."""
    with open("tests/data/rmnp.geojson", "rb") as f:
        collection = json.load(f)

    geom = shape(collection["features"][0]["geometry"])

    # QGIS uses a geodesic area computation and WGS84 ellipsoid.
    qgis_ellipsoidal_area = 1117.433937055  # kilometer squared

    # We expect no more than a 0.0001 km^2 difference. That's .00001%.
    assert round(qgis_ellipsoidal_area, 4) == round(area(geom) / 1e6, 4)


@pytest.mark.parametrize(
    ["kwargs", "exp_distance"],
    [({}, 9648.6280), ({"projected": True}, 9648.6280), ({"projected": False}, 0.1)],
)
def test_distance(kwargs, exp_distance):
    """Distance measured properly."""
    assert round(exp_distance, 4) == round(
        distance(Point(0, 0), Point(0.1, 0), **kwargs), 4
    )


@pytest.mark.parametrize(
    ["kwargs", "distance", "exp_area"],
    [
        ({}, 1.0e4, 312e6),
        ({"projected": True}, 10000.0, 312e6),
        ({"projected": False}, 0.1, 0.0312),
    ],
)
def test_buffer(kwargs, distance, exp_area):
    """Check area of a point buffered by 10km using 8 quadrant segments, should be ~312 km2."""
    # float(f"{x:.3g}") is used to round x to 3 significant figures.
    assert exp_area == float(
        f"{area(buffer(Point(0, 0), distance, **kwargs), **kwargs):.3g}"
    )


@pytest.mark.parametrize(
    ["kwargs", "exp_length"],
    [({}, 9648.6280), ({"projected": True}, 9648.6280), ({"projected": False}, 0.1)],
)
def test_length(kwargs, exp_length):
    """Length measured properly."""
    assert round(exp_length, 4) == round(
        length(LineString([(0, 0), (0.1, 0)]), **kwargs), 4
    )


@pytest.mark.parametrize(
    ["in_xy", "exp_xy", "kwargs"],
    [
        ((0.1, 0.0), (9648.628, 0.0), {}),
        ((0.1, 0.0), (9648.628, 0.0), {"projected": True}),
        ((0.1, 0.0), (0.1, 0.0), {"projected": False}),
    ],
)
def test_unary_property_wrapper(in_xy, exp_xy, kwargs):
    """Correctly wraps a function like shapely.area."""

    def func(geom, *args, **kwargs):
        """Echoes its input."""
        return geom, args, kwargs

    wrapper = unary_projectable_property_wrapper(func)
    assert wrapper.__doc__ == "Echoes its input."
    assert wrapper.__name__ == "func"
    g, *rest = wrapper(Point(*in_xy), "hello", this=True, **kwargs)
    assert rest == [("hello",), {"this": True}]
    assert round(g.x, 4) == round(exp_xy[0], 4)
    assert round(g.y, 4) == round(exp_xy[1], 4)


@pytest.mark.parametrize(
    ["in_xy", "exp_xy", "kwargs"],
    [
        ((0.1, 0.0), (9648.628, 0.0), {}),
        ((0.1, 0.0), (9648.628, 0.0), {"projected": True}),
        ((0.1, 0.0), (0.1, 0.0), {"projected": False}),
    ],
)
def test_unary_projectable_constructive_wrapper(in_xy, exp_xy, kwargs):
    """Correctly wraps a function like shapely.buffer."""

    def func(geom, required, this=False):
        """Echoes its input geom."""
        assert round(geom.x, 4) == round(exp_xy[0], 4)
        assert round(geom.y, 4) == round(exp_xy[1], 4)
        assert this is True
        return geom

    wrapper = unary_projectable_constructive_wrapper(func)
    assert wrapper.__doc__ == "Echoes its input geom."
    assert wrapper.__name__ == "func"
    g = wrapper(Point(*in_xy), "hello", this=True, **kwargs)
    assert round(g.x, 4) == round(in_xy[0], 4)
    assert round(g.y, 4) == round(in_xy[1], 4)


@pytest.mark.parametrize(
    ["in_xy", "exp_xy", "kwargs"],
    [
        ((0.1, 0.0), (9648.628, 0.0), {}),
        ((0.1, 0.0), (9648.628, 0.0), {"projected": True}),
        ((0.1, 0.0), (0.1, 0.0), {"projected": False}),
    ],
)
def test_binary_projectable_property_wrapper(in_xy, exp_xy, kwargs):
    """Correctly wraps a function like shapely.distance."""

    def func(geom1, geom2, *args, **kwargs):
        """Echoes its inputs."""
        return geom1, geom2, args, kwargs

    wrapper = binary_projectable_property_wrapper(func)
    assert wrapper.__doc__ == "Echoes its inputs."
    assert wrapper.__name__ == "func"
    g1, g2, *rest = wrapper(Point(*in_xy), Point(*in_xy), "hello", this=True, **kwargs)
    assert rest == [("hello",), {"this": True}]
    assert round(g1.x, 4) == round(exp_xy[0], 4)
    assert round(g1.y, 4) == round(exp_xy[1], 4)
    assert round(g2.x, 4) == round(exp_xy[0], 4)
    assert round(g2.y, 4) == round(exp_xy[1], 4)
