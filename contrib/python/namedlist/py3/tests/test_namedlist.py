#######################################################################
# Tests for namedlist module.
#
# Copyright 2011-2014 True Blade Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Notes:
#  When moving to a newer version of unittest, check that the exceptions
# being caught have the expected text in them.
#
########################################################################

from namedlist import namedlist, namedtuple, FACTORY, NO_DEFAULT

import sys
import copy
import unittest
import collections
import unicodedata

try:
    import collections.abc as collections_abc
except ImportError:
    import collections as collections_abc

_PY2 = sys.version_info[0] == 2
_PY3 = sys.version_info[0] == 3

# test both pickle and cPickle in 2.x, but just pickle in 3.x
import pickle
try:
    import cPickle
    pickle_modules = (pickle, cPickle)
except ImportError:
    pickle_modules = (pickle,)

# types used for pickle tests
TestNL0 = namedlist('TestNL0', '')
TestNL = namedlist('TestNL', 'x y z')

class TestNamedList(unittest.TestCase):
    def test_simple(self):
        Point = namedlist('Point', 'x y')
        p = Point(10, 20)
        self.assertEqual((p.x, p.y), (10, 20))
        self.assertEqual(p._asdict(), {'x':10, 'y':20})

        Point = namedlist('Point', 'x,y')
        p = Point(10, 20)
        self.assertEqual((p.x, p.y), (10, 20))
        self.assertEqual(p._asdict(), {'x':10, 'y':20})

        Point = namedlist('Point', 'x, y')
        p = Point(10, 20)
        self.assertEqual((p.x, p.y), (10, 20))
        self.assertEqual(p._asdict(), {'x':10, 'y':20})

        Point = namedlist('Point', ['x', 'y'])
        p = Point(10, 20)
        self.assertEqual((p.x, p.y), (10, 20))
        self.assertEqual(p._asdict(), {'x':10, 'y':20})

        self.assertEqual(Point(10, 11), Point(10, 11))
        self.assertNotEqual(Point(10, 11), Point(10, 12))

        self.assertEqual(vars(p), p._asdict())                              # verify that vars() works

    def test_unicode_identifiers(self):
        Point = namedlist(u'Point', u'x y')
        p = Point(10, 20)
        self.assertEqual((p.x, p.y), (10, 20))
        self.assertEqual(p._asdict(), {'x':10, 'y':20})

    def test_asdict_vars_ordered(self):
        Point = namedlist('Point', ['x', 'y'])
        p = Point(10, 20)

        # can't use unittest.skipIf in 2.6
        if sys.version_info[0] <= 2 and sys.version_info[1] <= 6:
            self.assertIsInstance(p.__dict__, dict)
        else:
            self.assertIsInstance(p.__dict__, collections.OrderedDict)

    def test_bad_name(self):
        self.assertRaises(ValueError, namedlist, 'Point*', 'x y')
        self.assertRaises(ValueError, namedlist, 'Point', '# y')
        self.assertRaises(ValueError, namedlist, 'Point', 'x 1y')
        self.assertRaises(ValueError, namedlist, 'Point', 'x y x')
        self.assertRaises(ValueError, namedlist, 'Point', 'x y for')
        self.assertRaises(ValueError, namedlist, 'Point', '_field')
        self.assertRaises(ValueError, namedlist, 'Point', [('', 0)])
        self.assertRaises(ValueError, namedlist, '', 'x y')
        self.assertRaises(ValueError, namedlist, 'Point', unicodedata.lookup('SUPERSCRIPT ONE'))

    def test_bad_defaults(self):
        # if specifying the defaults, must provide a 2-tuple
        self.assertRaises(ValueError, namedlist, 'Point', [('x', 3, 4)])
        self.assertRaises(ValueError, namedlist, 'Point', [('x',)])
        self.assertRaises(ValueError, namedlist, 'Point', [3])

    def test_empty(self):
        Point = namedlist('Point', '')
        self.assertEqual(len(Point()), 0)
        self.assertEqual(list(Point()), [])
        self.assertEqual(Point(), Point())
        self.assertEqual(Point()._asdict(), {})

        Point = namedlist('Point', '', 10)
        self.assertEqual(len(Point()), 0)
        self.assertEqual(Point(), Point())
        self.assertEqual(Point()._asdict(), {})

        Point = namedlist('Point', [])
        self.assertEqual(len(Point()), 0)
        self.assertEqual(Point(), Point())
        self.assertEqual(Point()._asdict(), {})

        Point = namedlist('Point', [], 10)
        self.assertEqual(len(Point()), 0)
        self.assertEqual(Point(), Point())
        self.assertEqual(Point()._asdict(), {})

    def test_list(self):
        Point = namedlist('Point', ['x', 'y'])
        p = Point(10, 20)
        self.assertEqual((p.x, p.y), (10, 20))
        self.assertEqual(p._asdict(), {'x':10, 'y':20})

        Point = namedlist('Point', ('x', 'y'))
        p = Point(10, 20)
        self.assertEqual((p.x, p.y), (10, 20))
        self.assertEqual(p._asdict(), {'x':10, 'y':20})

    def test_default(self):
        Point = namedlist('Point', 'x y z', 100)
        self.assertEqual(Point(), Point(100, 100, 100))
        self.assertEqual(Point(10), Point(10, 100, 100))
        self.assertEqual(Point(10, 20), Point(10, 20, 100))
        self.assertEqual(Point(10, 20, 30), Point(10, 20, 30))
        self.assertEqual(Point()._asdict(), {'x':100, 'y':100, 'z':100})

    def test_default_list(self):
        Point = namedlist('Point', 'x y z'.split(), 100)
        self.assertEqual(Point(), Point(100, 100, 100))
        self.assertEqual(Point(10), Point(10, 100, 100))
        self.assertEqual(Point(10, 20), Point(10, 20, 100))
        self.assertEqual(Point(10, 20, 30), Point(10, 20, 30))
        self.assertEqual(Point()._asdict(), {'x':100, 'y':100, 'z':100})

    def test_default_and_specified_default(self):
        Point = namedlist('Point', ['x', ('y', 10), ('z', 20)], 100)
        self.assertEqual(Point(), Point(100, 10, 20))
        self.assertEqual(Point(0), Point(0, 10, 20))
        self.assertEqual(Point(0, 1), Point(0, 1, 20))
        self.assertEqual(Point(0, 1, 2), Point(0, 1, 2))

        # default doesn't just have to apply to the last field
        Point = namedlist('Point', [('x', 0), 'y', ('z', 20)], 100)
        self.assertEqual(Point(), Point(0, 100, 20))

    def test_equality_inequality(self):
        Point = namedlist('Point', ['x', ('y', 10), ('z', 20)], 100)
        p0 = Point()
        p1 = Point(0)
        self.assertEqual(p0, Point())
        self.assertEqual(p0, Point(100, 10, 20))
        self.assertEqual(p1, Point(0, 10))
        self.assertEqual(Point(), p0)
        self.assertEqual(p0, p0)
        self.assertNotEqual(p0, p1)
        self.assertNotEqual(p0, 3)
        self.assertNotEqual(p0, None)
        self.assertNotEqual(p0, object())
        self.assertNotEqual(p0, Point('100'))
        self.assertNotEqual(p0, Point(100, 10, 21))

    def test_default_order(self):
        # with no default, can't have a field without a
        #  default follow fields with defaults
        self.assertRaises(ValueError, namedlist, 'Point',
                          ['x', ('y', 10), 'z'])

        # but with a default, you can
        Point = namedlist('Point', ['x', ('y', 10), 'z'], -1)
        self.assertEqual(Point(0), Point(0, 10, -1))
        self.assertEqual(Point(z=0), Point(-1, 10, 0))

    def test_repr(self):
        Point = namedlist('Point', 'x y z')
        p = Point(1, 2, 3)
        self.assertEqual(repr(p), 'Point(x=1, y=2, z=3)')
        self.assertEqual(str(p), 'Point(x=1, y=2, z=3)')

    def test_missing_argument(self):
        Point = namedlist('Point', ['x', 'y', ('z', 10)])
        self.assertEqual(Point(1, 2), Point(1, 2, 10))
        self.assertRaises(TypeError, Point, 1)

    def test_identity_of_defaults(self):
        default = object()
        Point = namedlist('Point', [('x', default)])
        # in 2.7 this should become assertIs
        self.assertTrue(Point().x is default)

        Point = namedlist('Point', 'x', default)
        # in 2.7 this should become assertIs
        self.assertTrue(Point().x is default)

    def test_writable(self):
        Point = namedlist('Point', ['x', ('y', 10), ('z', 20)], 100)
        p = Point(0)
        self.assertEqual((p.x, p.y, p.z), (0, 10, 20))
        p.x = -1
        self.assertEqual((p.x, p.y, p.z), (-1, 10, 20))
        p.y = -1
        self.assertEqual((p.x, p.y, p.z), (-1, -1, 20))
        p.z = None
        self.assertEqual((p.x, p.y, p.z), (-1, -1, None))

    def test_update_nothing(self):
        Point = namedlist('Point', ['x', ('y', 10), ('z', 20)], 100)

        a = Point(1)
        a._update()
        self.assertEqual((a.x, a.y, a.z), (1, 10, 20))

    def test_update_other_named_list(self):
        Point = namedlist('Point', ['x', ('y', 10), ('z', 20)], 100)

        a = Point(10)
        b = Point(100, 200, 300)
        a._update(b)

        self.assertEqual((a.x, a.y, a.z), (100, 200, 300))
        self.assertIsNot(a, b)

    def test_update_other_pairs(self):
        Point = namedlist('Point', ['x', ('y', 10), ('z', 20)], 100)

        a = Point(10)
        a._update([('y', 23), ('z', 42)])
        self.assertEqual((a.x, a.y, a.z), (10, 23, 42))

    def test_update_other_dict(self):
        Point = namedlist('Point', ['x', ('y', 10), ('z', 20)], 100)

        a = Point(10)
        a._update({'x': 32, 'y': 64})
        self.assertEqual((a.x, a.y, a.z), (32, 64, 20))

    def test_update_other_kwargs(self):
        Point = namedlist('Point', ['x', ('y', 10), ('z', 20)], 100)

        a = Point(10)
        a._update(x=60, z=90)
        self.assertEqual((a.x, a.y, a.z), (60, 10, 90))

    def test_update_other_pairs_kwargs(self):
        Point = namedlist('Point', ['x', ('y', 10), ('z', 20)], 100)

        a = Point(10)
        a._update([('y', 23)], z=42)
        self.assertEqual((a.x, a.y, a.z), (10, 23, 42))

    def test_update_other_dict_kwargs(self):
        Point = namedlist('Point', ['x', ('y', 10), ('z', 20)], 100)

        a = Point(10)
        a._update({'x': 32}, y=64)
        self.assertEqual((a.x, a.y, a.z), (32, 64, 20))

    def test_update_other_self_kwarg(self):
        # make sure 'self' and 'other' work as kwargs
        Point = namedlist('Point', 'self other')

        a = Point(1, 0)
        a._update(self=10, other=20)
        self.assertEqual((a.self, a.other), (10, 20))

    def test_replace_none(self):
        Point = namedlist('Point', 'x y z')
        a = Point(1, 2, 3)
        b = a._replace()
        self.assertIsNot(a, b)
        self.assertEqual((a.x, a.y, a.z), (1, 2, 3))
        self.assertEqual((b.x, b.y, b.z), (1, 2, 3))

    def test_replace_none(self):
        # make sure 'self' and 'other' work as kwargs
        Point = namedlist('Point', 'self other')
        a = Point(1, 2)
        b = a._replace(self=3, other=4)
        self.assertIsNot(a, b)
        self.assertEqual((a.self, a.other), (1, 2))
        self.assertEqual((b.self, b.other), (3, 4))

    def test_complex_defaults(self):
        Point = namedlist('Point', ['x', ('y', 10), ('z', 20)],
                          [1, 2, 3])
        p = Point()
        self.assertEqual((p.x, p.y, p.z), ([1, 2, 3], 10, 20))

        Point = namedlist('Point', [('x', [4, 5, 6]),
                                    ('y', 10),
                                    ('z', 20)])
        p = Point()
        self.assertEqual((p.x, p.y, p.z), ([4, 5, 6], 10, 20))

    def test_iteration(self):
        Point = namedlist('Point', ['x', ('y', 10), ('z', 20)],
                           [1, 2, 3])
        p = Point()
        self.assertEqual(len(p), 3)

        self.assertEqual(list(iter(p)), [[1, 2, 3], 10, 20])

        for expected, found in zip([[1, 2, 3], 10, 20], p):
            self.assertEqual(expected, found)

    def test_fields(self):
        Point = namedlist('Point', 'x y z')
        self.assertEqual(Point._fields, ('x', 'y', 'z'))
        self.assertIsInstance(Point._fields, tuple)

        Point = namedlist('Point', 'x y z', 100)
        self.assertEqual(Point._fields, ('x', 'y', 'z'))

        Point = namedlist('Point', [('x', 0), ('y', 0), ('z', 0)])
        self.assertEqual(Point._fields, ('x', 'y', 'z'))

        Point = namedlist('Point', '')
        self.assertEqual(Point._fields, ())
        self.assertIsInstance(Point._fields, tuple)

    def test_pickle(self):
        for p in (TestNL0(), TestNL(x=10, y=20, z=30)):
            for module in pickle_modules:
                for protocol in range(-1, module.HIGHEST_PROTOCOL + 1):
                    q = module.loads(module.dumps(p, protocol))
                    self.assertEqual(p, q)
                    self.assertEqual(p._fields, q._fields)
                    self.assertNotIn(b'OrderedDict', module.dumps(p, protocol))

    def test_type_has_same_name_as_field(self):
        Point = namedlist('Point',
                           ['Point', ('y', 10), ('z', 20)],
                           [1, 2, 3])
        p = Point()
        self.assertEqual(len(p), 3)
        self.assertEqual(p.Point, [1, 2, 3])

        Point = namedlist('Point', 'Point')
        p = Point(4)
        self.assertEqual(p.Point, 4)

        Point = namedlist('Point', 'x Point')
        p = Point(3, 4)
        self.assertEqual(p.Point, 4)

    def test_slots(self):
        Point = namedlist('Point', '')
        p = Point()
        # p.x = 3 raises AttributeError because of slots
        self.assertRaises(AttributeError, setattr, p, 'x', 3)

        Point = namedlist('Point', '', use_slots=True)
        p = Point()
        # p.x = 3 raises AttributeError because of slots
        self.assertRaises(AttributeError, setattr, p, 'x', 3)

    def test_no_slots(self):
        Point = namedlist('Point', '', use_slots=False)
        p = Point()
        # we should be able to create new attributes
        p.x = 3
        self.assertEqual(p.x, 3)

    def test_rename(self):
        Point = namedlist('Point', ('abc', 'def'), rename=True)
        self.assertEqual(Point._fields, ('abc', '_1'))

        Point = namedlist('Point', ('for', 'def'), rename=True)
        self.assertEqual(Point._fields, ('_0', '_1'))

        Point = namedlist('Point', 'a a b a b c', rename=True)
        self.assertEqual(Point._fields, ('a', '_1', 'b', '_3', '_4', 'c'))

        # nothing needs to be renamed, should still work with rename=True
        Point = namedlist('Point', 'x y z', rename=True)
        self.assertEqual(Point._fields, ('x', 'y', 'z'))

        Point = namedlist('Point', 'x y _z', rename=True)
        self.assertEqual(Point._fields, ('x', 'y', '_2'))

        # rename with defaults
        Point = namedlist('Point', [('', 1), ('', 2)], rename=True)
        p = Point()
        self.assertEqual(p._0, 1)
        self.assertEqual(p._1, 2)

    def test_type_begins_with_underscore(self):
        Point = namedlist('_Point', '')
        p = Point()

    def test_mapping(self):
        # use a regular dict so testing with 2.6 is still possible
        # do not make any assumptions about field order
        Point = namedlist('Point', {'x': 0, 'y': 100})
        p = Point()
        self.assertEqual(p.x, 0)
        self.assertEqual(p.y, 100)

        # in 2.7, test with an OrderedDict

    def test_NO_DEFAULT(self):
        # NO_DEFAULT is only really useful with we're using a mapping
        #  plus a default value. it's the only way to specify that
        #  some of the fields use the default.
        Point = namedlist('Point', {'x':0, 'y':NO_DEFAULT}, default=5)
        p = Point()
        self.assertEqual(p.x, 0)
        self.assertEqual(p.y, 5)

    def test_iterable(self):
        Point = namedlist('Point', iter(['x', 'y']))
        p = Point(1, 2)
        self.assertEqual(p.x, 1)
        self.assertEqual(p.y, 2)

    def test_single_field(self):
        X = namedlist('X', 'xyz')
        self.assertEqual(X._fields, ('xyz',))

    def test_repr_output(self):
        Point = namedlist('Point', 'a b')
        p = Point('0', 0)
        self.assertEqual(repr(p), "Point(a='0', b=0)")

    def test_mutable_defaults(self):
        # this behavior is unfortunate, but it should be tested anyway
        A = namedlist('A', [('x', [])])
        a = A()
        self.assertEqual(a.x, [])

        a.x.append(4)
        self.assertEqual(a.x, [4])

        b = A()
        self.assertEqual(b.x, [4])

    def test_factory_functions(self):
        A = namedlist('A', [('x', FACTORY(list))])
        a = A()
        self.assertEqual(a.x, [])

        a.x.append(4)
        self.assertEqual(a.x, [4])

        b = A()
        self.assertEqual(b.x, [])

    def test_factory_for_default(self):
        # make sure FACTORY works for the global default
        A = namedlist('A', 'x y', default=FACTORY(list))
        a = A()
        self.assertEqual(a.x, [])
        self.assertEqual(a.y, [])

        a.x.append(4)
        self.assertEqual(a.x, [4])
        a.y.append(4)
        self.assertEqual(a.y, [4])

        b = A()
        self.assertEqual(b.x, [])
        self.assertEqual(b.y, [])

        # mix and match FACTORY and a non-callable mutable default
        A = namedlist('A', [('x', []), 'y'], default=FACTORY(list))
        a = A()
        self.assertEqual(a.x, [])
        self.assertEqual(a.y, [])

        a.x.append(4)
        self.assertEqual(a.x, [4])
        a.y.append(4)
        self.assertEqual(a.y, [4])

        b = A()
        self.assertEqual(b.x, [4])
        self.assertEqual(b.y, [])

    def test_unhashable(self):
        Point = namedlist('Point', 'a b')
        p = Point(1, 2)
        self.assertRaises(TypeError, hash, p)

    def test_getitem(self):
        Point = namedlist('Point', 'a b')
        p = Point(1, 2)
        self.assertEqual((p[0], p[1]), (1, 2))
        self.assertEqual(list(p), [1, 2])
        self.assertRaises(IndexError, p.__getitem__, 2)

    def test_setitem(self):
        Point = namedlist('Point', 'a b')
        p = Point(1, 2)
        p[0] = 10
        self.assertEqual(list(p), [10, 2])
        p[1] = 20
        self.assertEqual(list(p), [10, 20])
        self.assertRaises(IndexError, p.__setitem__, 2, 3)

    def test_container(self):
        # I'm not sure there's much sense in this, but list is a container
        Point = namedlist('Point', 'a b')
        p = Point(1, 2)
        self.assertIn(2, p)

    def test_ABC(self):
        Point = namedlist('Point', 'a b c')
        p = Point(1, 2, 2)
        self.assertIsInstance(p, collections_abc.Container)
        self.assertIsInstance(p, collections_abc.Iterable)
        self.assertIsInstance(p, collections_abc.Sized)
        self.assertIsInstance(p, collections_abc.Sequence)

        self.assertEqual(list(reversed(p)), [2, 2, 1])
        self.assertEqual(p.count(0), 0)
        self.assertEqual(p.count(2), 2)
        self.assertRaises(ValueError, p.index, 0)
        self.assertEqual(p.index(2), 1)
        self.assertEqual(p.index(1), 0)

        A = namedlist('A', 'a b c d e f g h i j')
        a = A(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        self.assertEqual(a.index(0, 1), 1)
        self.assertEqual(a.index(0, 5), 5)
        self.assertEqual(a.index(0, 1, 3), 1)
        self.assertEqual(a.index(0, 5, 12), 5)
        self.assertRaises(ValueError, a.index, 0, 12)

    def test_docstring(self):
        Point = namedlist('Point', '')
        self.assertEqual(Point.__doc__, 'Point()')

        Point = namedlist('Point', 'dx')
        self.assertEqual(Point.__doc__, 'Point(dx)')

        Point = namedlist('Point', 'x')
        self.assertEqual(Point.__doc__, 'Point(x)')

        Point = namedlist('Point', 'dx dy, dz')
        self.assertEqual(Point.__doc__, 'Point(dx, dy, dz)')

        Point = namedlist('Point', 'dx dy dz', default=10)
        self.assertEqual(Point.__doc__, 'Point(dx=10, dy=10, dz=10)')

        Point = namedlist('Point', 'dx, dy, dz', default=FACTORY(10))
        self.assertEqual(Point.__doc__, 'Point(dx=FACTORY(10), dy=FACTORY(10), dz=FACTORY(10))')

        Point = namedlist('Point', ['dx', 'dy', ('dz', 11.0)], default=10)
        self.assertEqual(Point.__doc__, 'Point(dx=10, dy=10, dz=11.0)')

        Point = namedlist('Point', ['dx', 'dy', ('dz', 11.0)], default=FACTORY(list))
        if _PY2:
            list_repr = "<type 'list'>"
        else:
            list_repr = "<class 'list'>"
        self.assertEqual(Point.__doc__, "Point(dx=FACTORY({0}), dy=FACTORY({0}), dz=11.0)".format(list_repr))

        Point = namedlist('Point', ['dx', 'dy', ('dz', FACTORY(11.0))], default=[])
        self.assertEqual(Point.__doc__, 'Point(dx=[], dy=[], dz=FACTORY(11.0))')

    def test_slice(self):
        Point = namedlist('Point', 'x y z color')
        values = [3, 5, -12, 'red']
        p = Point(*values)
        self.assertEqual(values[0:-1], p[0:-1])
        self.assertEqual(values[:3], p[:3])
        self.assertEqual(values[4:1:-1], p[4:1:-1])



TestNT = namedtuple('TestNT', 'x y z')    # type used for pickle tests

class TestNamedTuple(unittest.TestCase):

    def test_unicode_identifiers(self):
        Point = namedtuple(u'Point', u'x y')
        p = Point(10, 20)
        self.assertEqual((p.x, p.y), (10, 20))
        self.assertEqual(p._asdict(), {'x':10, 'y':20})

    def test_factory(self):
        Point = namedtuple('Point', 'x y')
        self.assertEqual(Point.__name__, 'Point')
        self.assertEqual(Point.__slots__, ())
        self.assertEqual(Point.__module__, __name__)
        self.assertEqual(Point.__getitem__, tuple.__getitem__)
        self.assertEqual(Point._fields, ('x', 'y'))

        self.assertRaises(ValueError, namedtuple, 'abc%', 'efg ghi')       # type has non-alpha char
        self.assertRaises(ValueError, namedtuple, 'class', 'efg ghi')      # type has keyword
        self.assertRaises(ValueError, namedtuple, '9abc', 'efg ghi')       # type starts with digit

        self.assertRaises(ValueError, namedtuple, 'abc', 'efg g%hi')       # field with non-alpha char
        self.assertRaises(ValueError, namedtuple, 'abc', 'abc class')      # field has keyword
        self.assertRaises(ValueError, namedtuple, 'abc', '8efg 9ghi')      # field starts with digit
        self.assertRaises(ValueError, namedtuple, 'abc', '_efg ghi')       # field with leading underscore
        self.assertRaises(ValueError, namedtuple, 'abc', 'efg efg ghi')    # duplicate field

        namedtuple('Point0', 'x1 y2')   # Verify that numbers are allowed in names
        namedtuple('_', 'a b c')        # Test leading underscores in a typename

        nt = namedtuple('nt', 'the quick brown fox')                       # check unicode input
        self.assertNotIn("u'", repr(nt._fields))
        nt = namedtuple('nt', ('the', 'quick'))                           # check unicode input
        self.assertNotIn("u'", repr(nt._fields))

        self.assertRaises(TypeError, Point._make, [11])                     # catch too few args
        self.assertRaises(TypeError, Point._make, [11, 22, 33])             # catch too many args

    @unittest.skipIf(sys.flags.optimize >= 2,
                     "Docstrings are omitted with -O2 and above")
    def test_factory_doc_attr(self):
        Point = namedtuple('Point', 'x y')
        self.assertEqual(Point.__doc__, 'Point(x, y)')

    def test_name_fixer(self):
        for spec, renamed in [
            [('efg', 'g%hi'),  ('efg', '_1')],                              # field with non-alpha char
            [('abc', 'class'), ('abc', '_1')],                              # field has keyword
            [('8efg', '9ghi'), ('_0', '_1')],                               # field starts with digit
            [('abc', '_efg'), ('abc', '_1')],                               # field with leading underscore
            [('abc', 'efg', 'efg', 'ghi'), ('abc', 'efg', '_2', 'ghi')],    # duplicate field
            [('abc', '', 'x'), ('abc', '_1', 'x')],                         # fieldname is a space
        ]:
            self.assertEqual(namedtuple('NT', spec, rename=True)._fields, renamed)

    def test_instance(self):
        Point = namedtuple('Point', 'x y')
        p = Point(11, 22)
        self.assertEqual(p, Point(x=11, y=22))
        self.assertEqual(p, Point(11, y=22))
        self.assertEqual(p, Point(y=22, x=11))
        self.assertEqual(p, Point(*(11, 22)))
        self.assertEqual(p, Point(**dict(x=11, y=22)))
        self.assertRaises(TypeError, Point, 1)                              # too few args
        self.assertRaises(TypeError, Point, 1, 2, 3)                        # too many args
        self.assertRaises(TypeError, eval, 'Point(XXX=1, y=2)', locals())   # wrong keyword argument
        self.assertRaises(TypeError, eval, 'Point(x=1)', locals())          # missing keyword argument
        self.assertEqual(repr(p), 'Point(x=11, y=22)')
        self.assertNotIn('__weakref__', dir(p))
        self.assertEqual(p, Point._make([11, 22]))                          # test _make classmethod
        self.assertEqual(p._fields, ('x', 'y'))                             # test _fields attribute
        self.assertEqual(p._replace(x=1), (1, 22))                          # test _replace method
        self.assertEqual(p._asdict(), dict(x=11, y=22))                     # test _asdict method
        self.assertEqual(vars(p), p._asdict())                              # verify that vars() works

        try:
            p._replace(x=1, error=2)
        except ValueError:
            pass
        else:
            self._fail('Did not detect an incorrect fieldname')

        # verify that field string can have commas
        Point = namedtuple('Point', 'x, y')
        p = Point(x=11, y=22)
        self.assertEqual(repr(p), 'Point(x=11, y=22)')

        # verify that fieldspec can be a non-string sequence
        Point = namedtuple('Point', ('x', 'y'))
        p = Point(x=11, y=22)
        self.assertEqual(repr(p), 'Point(x=11, y=22)')

    def test_tupleness(self):
        Point = namedtuple('Point', 'x y')
        p = Point(11, 22)

        self.assertIsInstance(p, tuple)
        self.assertEqual(p, (11, 22))                                       # matches a real tuple
        self.assertEqual(tuple(p), (11, 22))                                # coercable to a real tuple
        self.assertEqual(list(p), [11, 22])                                 # coercable to a list
        self.assertEqual(max(p), 22)                                        # iterable
        self.assertEqual(max(*p), 22)                                       # star-able
        x, y = p
        self.assertEqual(p, (x, y))                                         # unpacks like a tuple
        self.assertEqual((p[0], p[1]), (11, 22))                            # indexable like a tuple
        self.assertRaises(IndexError, p.__getitem__, 3)

        self.assertEqual(p.x, x)
        self.assertEqual(p.y, y)
        self.assertRaises(AttributeError, eval, 'p.z', locals())

    def test_odd_sizes(self):
        Zero = namedtuple('Zero', '')
        self.assertEqual(Zero(), ())
        self.assertEqual(Zero._make([]), ())
        self.assertEqual(repr(Zero()), 'Zero()')
        self.assertEqual(Zero()._asdict(), {})
        self.assertEqual(Zero()._fields, ())

        Dot = namedtuple('Dot', 'd')
        self.assertEqual(Dot(1), (1,))
        self.assertEqual(Dot._make([1]), (1,))
        self.assertEqual(Dot(1).d, 1)
        self.assertEqual(repr(Dot(1)), 'Dot(d=1)')
        self.assertEqual(Dot(1)._asdict(), {'d':1})
        self.assertEqual(Dot(1)._replace(d=999), (999,))
        self.assertEqual(Dot(1)._fields, ('d',))

        # n = 5000
        n = 254 # SyntaxError: more than 255 arguments:
        import string, random
        names = list(set(''.join([random.choice(string.ascii_letters)
                                  for j in range(10)]) for i in range(n)))
        n = len(names)
        Big = namedtuple('Big', names)
        b = Big(*range(n))
        self.assertEqual(b, tuple(range(n)))
        self.assertEqual(Big._make(range(n)), tuple(range(n)))
        for pos, name in enumerate(names):
            self.assertEqual(getattr(b, name), pos)
        repr(b)                                 # make sure repr() doesn't blow-up
        d = b._asdict()
        d_expected = dict(zip(names, range(n)))
        self.assertEqual(d, d_expected)
        b2 = b._replace(**dict([(names[1], 999),(names[-5], 42)]))
        b2_expected = list(range(n))
        b2_expected[1] = 999
        b2_expected[-5] = 42
        self.assertEqual(b2, tuple(b2_expected))
        self.assertEqual(b._fields, tuple(names))

    def test_pickle(self):
        p = TestNT(x=10, y=20, z=30)
        for module in (pickle,):
            loads = getattr(module, 'loads')
            dumps = getattr(module, 'dumps')
            for protocol in -1, 0, 1, 2:
                q = loads(dumps(p, protocol))
                self.assertEqual(p, q)
                self.assertEqual(p._fields, q._fields)
                self.assertNotIn(b'OrderedDict', dumps(p, protocol))

    def test_copy(self):
        p = TestNT(x=10, y=20, z=30)
        for copier in copy.copy, copy.deepcopy:
            q = copier(p)
            self.assertEqual(p, q)
            self.assertEqual(p._fields, q._fields)

    def test_name_conflicts(self):
        # Some names like "self", "cls", "tuple", "itemgetter", and "property"
        # failed when used as field names.  Test to make sure these now work.
        T = namedtuple('T', 'itemgetter property self cls tuple')
        t = T(1, 2, 3, 4, 5)
        self.assertEqual(t, (1,2,3,4,5))
        newt = t._replace(itemgetter=10, property=20, self=30, cls=40, tuple=50)
        self.assertEqual(newt, (10,20,30,40,50))


    def test_repr(self):
        A = namedtuple('A', 'x')
        self.assertEqual(repr(A(1)), 'A(x=1)')
        # repr should show the name of the subclass
        class B(A):
            pass
        self.assertEqual(repr(B(1)), 'B(x=1)')


class TestAll(unittest.TestCase):
    def test_all(self):
        import namedlist

        # check that __all__ in the module contains everything that should be
        #  public, and only those symbols
        all = set(namedlist.__all__)

        # check that things in __all__ only appear once
        self.assertEqual(len(all), len(namedlist.__all__),
                         'some symbols appear more than once in __all__')

        # get the list of public symbols
        found = set(name for name in dir(namedlist) if not name.startswith('_'))

        # make sure it matches __all__
        self.assertEqual(all, found)



# 2.6 is missing some unittest.TestCase members. Add
#  trivial implementations for them.
def _assertIsInstance(self, obj, cls):
    self.assertTrue(isinstance(obj, cls))

def _assertIn(self, obj, iterable):
    self.assertTrue(obj in iterable)

def _assertNotIn(self, obj, iterable):
    self.assertTrue(not obj in iterable)

def _add_unittest_methods(cls):
    for name, fn in [('assertIsInstance', _assertIsInstance),
                     ('assertIn', _assertIn),
                     ('assertNotIn', _assertNotIn)]:
        if not hasattr(cls, name):
            setattr(cls, name, fn)

_add_unittest_methods(TestNamedList)

#unittest.main()
