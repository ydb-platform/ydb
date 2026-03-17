import unittest
import rstr.tests.test_rstr
import rstr.tests.test_xeger
import rstr.tests.test_package_level_access


def suite():
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(test_rstr)
    suite.addTests(loader.loadTestsFromModule(test_xeger))
    suite.addTests(loader.loadTestsFromModule(test_package_level_access))
    return suite
