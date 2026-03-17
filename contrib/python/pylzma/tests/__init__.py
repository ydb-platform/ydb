import unittest
import sys

# some Python 2.3 unittest compatibility fixes
if not hasattr(unittest.TestCase, 'assertTrue'):
    unittest.TestCase.assertTrue = unittest.TestCase.failUnless
if not hasattr(unittest.TestCase, 'assertFalse'):
    unittest.TestCase.assertFalse = unittest.TestCase.failIf
if not hasattr(unittest.TestCase, 'assertEqual'):
    unittest.TestCase.assertEqual = unittest.TestCase.failUnlessEqual
if not hasattr(unittest.TestCase, 'assertNotEqual'):
    unittest.TestCase.assertNotEqual = unittest.TestCase.failIfEqual
if not hasattr(unittest.TestCase, 'assertRaises'):
    unittest.TestCase.assertRaises = unittest.TestCase.failUnlessRaises

mod_base = 'tests'


def suite():
    import os.path
    from glob import glob

    suite = unittest.TestSuite()

    for testcase in glob(os.path.join(os.path.dirname(__file__), 'test_*.py')):
        if 'usage' in testcase and sys.version_info[:2] >= (3, 0):
            continue

        mod_name = os.path.basename(testcase).split('.')[0]
        full_name = '%s.%s' % (mod_base, mod_name)

        mod = __import__(full_name)

        for part in full_name.split('.')[1:]:
            mod = getattr(mod, part)

        suite.addTest(mod.suite())

    return suite


def main():
    unittest.main(defaultTest='suite')

if __name__ == '__main__':
    main()
