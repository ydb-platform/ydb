import unittest


class TestCase(unittest.TestCase):
    def assertRaises(self, ex, f, *args, **kwargs):
        try:
            f(*args, **kwargs)
        except ex as e:
            return e
        else:
            assert False, "%r didn't raise %r" % (f, ex)
