import unittest

from hjson.compat import StringIO
import hjson as json

class TestTuples(unittest.TestCase):
    def test_tuple_array_dumps(self):
        t = (1, 2, 3)
        expect = json.dumpsJSON(list(t))
        # Default is True
        self.assertEqual(expect, json.dumpsJSON(t))
        self.assertEqual(expect, json.dumpsJSON(t, tuple_as_array=True))
        self.assertRaises(TypeError, json.dumpsJSON, t, tuple_as_array=False)
        # Ensure that the "default" does not get called
        self.assertEqual(expect, json.dumpsJSON(t, default=repr))
        self.assertEqual(expect, json.dumpsJSON(t, tuple_as_array=True,
                                            default=repr))
        # Ensure that the "default" gets called
        self.assertEqual(
            json.dumpsJSON(repr(t)),
            json.dumpsJSON(t, tuple_as_array=False, default=repr))

    def test_tuple_array_dump(self):
        t = (1, 2, 3)
        expect = json.dumpsJSON(list(t))
        # Default is True
        sio = StringIO()
        json.dumpJSON(t, sio)
        self.assertEqual(expect, sio.getvalue())
        sio = StringIO()
        json.dumpJSON(t, sio, tuple_as_array=True)
        self.assertEqual(expect, sio.getvalue())
        self.assertRaises(TypeError, json.dumpJSON, t, StringIO(),
                          tuple_as_array=False)
        # Ensure that the "default" does not get called
        sio = StringIO()
        json.dumpJSON(t, sio, default=repr)
        self.assertEqual(expect, sio.getvalue())
        sio = StringIO()
        json.dumpJSON(t, sio, tuple_as_array=True, default=repr)
        self.assertEqual(expect, sio.getvalue())
        # Ensure that the "default" gets called
        sio = StringIO()
        json.dumpJSON(t, sio, tuple_as_array=False, default=repr)
        self.assertEqual(
            json.dumpsJSON(repr(t)),
            sio.getvalue())

class TestNamedTuple(unittest.TestCase):
    def test_namedtuple_dump(self):
        pass
