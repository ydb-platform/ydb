import numpy
import pytest

from thinc.layers import premap_ids, remap_ids, remap_ids_v2


@pytest.fixture
def keys():
    return numpy.array([4, 2, 6, 1, 8, 7, 9, 3, 30])


@pytest.fixture
def mapper(keys):
    return {int(k): int(v) for v, k in enumerate(keys)}


def test_premap(keys, mapper):
    premap = premap_ids(mapper, default=99)
    values, _ = premap(keys, False)
    numpy.testing.assert_equal(values.squeeze(), numpy.asarray(range(len(keys))))


def test_remap(keys, mapper):
    remap = remap_ids(mapper, default=99)
    values, _ = remap(keys, False)
    numpy.testing.assert_equal(values.squeeze(), numpy.asarray(range(len(keys))))


def test_remap_v2(keys, mapper):
    remap = remap_ids_v2(mapper, default=99)
    values, _ = remap(keys, False)
    numpy.testing.assert_equal(values.squeeze(), numpy.asarray(range(len(keys))))


def test_remap_premap_eq(keys, mapper):
    remap = remap_ids(mapper, default=99)
    remap_v2 = remap_ids_v2(mapper, default=99)
    premap = premap_ids(mapper, default=99)
    values1, _ = remap(keys, False)
    values2, _ = remap_v2(keys, False)
    values3, _ = premap(keys, False)
    numpy.testing.assert_equal(values1, values2)
    numpy.testing.assert_equal(values2, values3)


def test_column(keys, mapper):
    idx = numpy.zeros((len(keys), 4), dtype="int")
    idx[:, 3] = keys
    remap_v2 = remap_ids_v2(mapper, column=3)
    premap = premap_ids(mapper, column=3)
    numpy.testing.assert_equal(
        remap_v2(idx, False)[0].squeeze(), numpy.asarray(range(len(keys)))
    )
    numpy.testing.assert_equal(
        premap(idx, False)[0].squeeze(), numpy.asarray(range(len(keys)))
    )
