from __future__ import absolute_import
import unittest
import doctest
import sys


def additional_tests(suite=None):
    import hjson
    import hjson.encoder
    import hjson.decoder
    if suite is None:
        suite = unittest.TestSuite()
    for mod in (hjson, hjson.encoder, hjson.decoder):
        suite.addTest(doctest.DocTestSuite(mod))
    return suite


def all_tests_suite():
    def get_suite():
        return additional_tests(
            unittest.TestLoader().loadTestsFromNames([
                'hjson.tests.test_hjson',
                'hjson.tests.test_bitsize_int_as_string',
                'hjson.tests.test_bigint_as_string',
                'hjson.tests.test_check_circular',
                'hjson.tests.test_decode',
                'hjson.tests.test_default',
                'hjson.tests.test_dump',
                'hjson.tests.test_encode_basestring_ascii',
                'hjson.tests.test_errors',
                'hjson.tests.test_fail',
                'hjson.tests.test_float',
                'hjson.tests.test_indent',
                'hjson.tests.test_pass1',
                'hjson.tests.test_pass2',
                'hjson.tests.test_pass3',
                'hjson.tests.test_recursion',
                'hjson.tests.test_scanstring',
                'hjson.tests.test_separators',
                'hjson.tests.test_unicode',
                'hjson.tests.test_decimal',
                'hjson.tests.test_tuple',
                'hjson.tests.test_namedtuple',
                #'hjson.tests.test_tool', # fails on windows
                'hjson.tests.test_for_json',
            ]))
    suite = get_suite()
    return suite


def main():
    runner = unittest.TextTestRunner(verbosity=1 + sys.argv.count('-v'))
    suite = all_tests_suite()
    raise SystemExit(not runner.run(suite).wasSuccessful())


if __name__ == '__main__':
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    main()
