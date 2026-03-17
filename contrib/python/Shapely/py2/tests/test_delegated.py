from . import unittest
from shapely.geometry import Point
from shapely.impl import BaseImpl
from shapely.geometry.base import delegated


class Geometry(object):

    impl = BaseImpl({})

    @property
    @delegated
    def foo(self):
        return self.impl['foo']()


class WrapperTestCase(unittest.TestCase):
    """When the backend has no support for a method, we get an AttributeError
    """

    def test_delegated(self):
        self.assertRaises(AttributeError, getattr, Geometry(), 'foo')

    def test_defaultimpl(self):
        project_impl = Point.impl.map.pop('project', None)
        try:
            self.assertRaises(AttributeError, Point(0, 0).project, 1.0)
        finally:
            if project_impl is not None:
                Point.impl.map['project'] = project_impl


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(WrapperTestCase)
