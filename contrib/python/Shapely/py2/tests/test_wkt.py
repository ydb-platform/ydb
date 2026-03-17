from math import pi

import pytest

from shapely.geometry import LineString, Point
from shapely.wkt import dumps


@pytest.fixture(scope="module")
def pipi():
    return Point((pi, -pi))


@pytest.fixture(scope="module")
def pipi4():
    return Point((pi*4, -pi*4))


def test_wkt(pipi):
    """.wkt and wkt.dumps() both do not trim by default."""
    assert pipi.wkt == "POINT ({0:.16f} {1:.16f})".format(pi, -pi)


def test_wkt(pipi4):
    """.wkt and wkt.dumps() both do not trim by default."""
    assert pipi4.wkt == "POINT ({0:.16f} {1:.16f})".format(pi*4, -pi*4)


def test_dumps(pipi4):
    assert dumps(pipi4) == "POINT ({0:.16f} {1:.16f})".format(pi*4, -pi*4)


def test_dumps_precision(pipi4):
    assert dumps(pipi4, rounding_precision=4) == "POINT ({0:.4f} {1:.4f})".format(pi*4, -pi*4)
