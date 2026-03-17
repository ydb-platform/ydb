import os
import doctest
from . import unittest
from glob import glob

optionflags = (doctest.REPORT_ONLY_FIRST_FAILURE |
               doctest.NORMALIZE_WHITESPACE |
               doctest.ELLIPSIS)


def list_doctests():
    print(__file__)
    source_files = glob(os.path.join(os.path.dirname(__file__), '*.txt'))
    return [filename for filename in source_files]


def open_file(filename, mode='r'):
    """Helper function to open files from within the tests package."""
    return open(os.path.join(os.path.dirname(__file__), filename), mode)


def setUp(test):
    test.globs.update(dict(open_file=open_file,))


def _test_suite():
    return unittest.TestSuite(
        [doctest.DocFileSuite(os.path.basename(filename),
                              optionflags=optionflags,
                              setUp=setUp)
         for filename
         in list_doctests()])

if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=1)
    runner.run(test_suite())
