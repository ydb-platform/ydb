import pytest

from shapely.geometry import Point
from shapely.impl import delegated, ImplementationError


def test_error():
    with pytest.raises(ImplementationError):
        Point(0, 0).impl['bogus']()
    with pytest.raises(NotImplementedError):
        Point(0, 0).impl['bogus']()
    with pytest.raises(KeyError):
        Point(0, 0).impl['bogus']()


def test_delegated():
    class Poynt(Point):
        @delegated
        def bogus(self):
            return self.impl['bogus']()
    with pytest.raises(ImplementationError):
        Poynt(0, 0).bogus()
    with pytest.raises(AttributeError):
        Poynt(0, 0).bogus()
