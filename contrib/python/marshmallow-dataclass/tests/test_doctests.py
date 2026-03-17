import doctest

import marshmallow_dataclass


def load_tests(_loader, tests, _ignore):
    # Load all the doctests defined in the module
    tests.addTests(doctest.DocTestSuite(marshmallow_dataclass))
    return tests
