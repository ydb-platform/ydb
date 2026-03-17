import sys
import appdirs

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

if sys.version_info[0] < 3:
    STRING_TYPE = basestring
else:
    STRING_TYPE = str


class Test_AppDir(unittest.TestCase):
    def test_metadata(self):
        self.assertTrue(hasattr(appdirs, "__version__"))
        self.assertTrue(hasattr(appdirs, "__version_info__"))

    def test_helpers(self):
        self.assertIsInstance(
            appdirs.user_data_dir('MyApp', 'MyCompany'), STRING_TYPE)
        self.assertIsInstance(
            appdirs.site_data_dir('MyApp', 'MyCompany'), STRING_TYPE)
        self.assertIsInstance(
            appdirs.user_cache_dir('MyApp', 'MyCompany'), STRING_TYPE)
        self.assertIsInstance(
            appdirs.user_state_dir('MyApp', 'MyCompany'), STRING_TYPE)
        self.assertIsInstance(
            appdirs.user_log_dir('MyApp', 'MyCompany'), STRING_TYPE)

    def test_dirs(self):
        dirs = appdirs.AppDirs('MyApp', 'MyCompany', version='1.0')
        self.assertIsInstance(dirs.user_data_dir, STRING_TYPE)
        self.assertIsInstance(dirs.site_data_dir, STRING_TYPE)
        self.assertIsInstance(dirs.user_cache_dir, STRING_TYPE)
        self.assertIsInstance(dirs.user_state_dir, STRING_TYPE)
        self.assertIsInstance(dirs.user_log_dir, STRING_TYPE)

if __name__ == "__main__":
    unittest.main()
