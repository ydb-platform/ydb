try:
    import unittest2 as unittest
except ImportError:
    import unittest


class TestCase(unittest.TestCase):
    """
    We use this base class for all the tests in this package.
    If necessary, we can put common utility or setup code in here.
    """

    maxDiff = 10 ** 10
