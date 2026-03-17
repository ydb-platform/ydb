'''Test locale independence of WKT
'''
from . import unittest
import sys
import locale
from shapely.wkt import loads, dumps

# Set locale to one that uses a comma as decimal seperator
# TODO: try a few other common locales
if sys.platform == 'win32':
    test_locales = {
        'Portuguese': 'portuguese_brazil',
    }
else:
    test_locales = {
        'Portuguese': 'pt_BR.UTF-8',
    }

do_test_locale = False


def setUpModule():
    global do_test_locale
    for name in test_locales:
        try:
            test_locale = test_locales[name]
            locale.setlocale(locale.LC_ALL, test_locale)
            do_test_locale = True
            break
        except:
            pass
    if not do_test_locale:
        raise unittest.SkipTest('test locale not found')


def tearDownModule():
    if sys.platform == 'win32':
        locale.setlocale(locale.LC_ALL, "")
    else:
        locale.resetlocale()


class LocaleTestCase(unittest.TestCase):

    #@unittest.skipIf(not do_test_locale, 'test locale not found')

    def test_wkt_locale(self):

        # Test reading and writing
        p = loads('POINT (0.0 0.0)')
        self.assertEqual(p.x, 0.0)
        self.assertEqual(p.y, 0.0)
        wkt = dumps(p)
        self.assertTrue(wkt.startswith('POINT'))
        self.assertFalse(',' in wkt)


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(LocaleTestCase)
