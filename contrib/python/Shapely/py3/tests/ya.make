PY3TEST()

PEERDIR(
    contrib/python/Shapely
)

NO_LINT()

SRCDIR(contrib/python/Shapely/py3/shapely/tests)

PY_NAMESPACE(shapely.tests)

PY_SRCS(
    common.py
)

TEST_SRCS(
    geometry/__init__.py
    geometry/test_collection.py
    geometry/test_coords.py
    geometry/test_decimal.py
    geometry/test_emptiness.py
    geometry/test_equality.py
    geometry/test_format.py
    geometry/test_geometry_base.py
    geometry/test_hash.py
    geometry/test_linestring.py
    geometry/test_multi.py
    geometry/test_multilinestring.py
    geometry/test_multipoint.py
    geometry/test_multipolygon.py
    geometry/test_point.py
    geometry/test_polygon.py
    __init__.py
#    test_constructive.py
    test_coordinates.py
    test_creation.py
    test_creation_indices.py
    test_geometry.py
    test_io.py
    test_linear.py
    test_measurement.py
    test_misc.py
#    test_plotting.py
    test_predicates.py
    test_ragged_array.py
    test_set_operations.py
    test_strtree.py
    test_testing.py
)

END()
