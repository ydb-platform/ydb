#!/usr/bin/env python

"""Unit tests for M2Crypto.Engine."""

from M2Crypto import Engine
from tests import unittest


class EngineTestCase(unittest.TestCase):

    privkey = 'tests/rsa.priv.pem'
    bad_id = '1bea1edfeb97'

    def tearDown(self):
        Engine.cleanup()

    def test_by_id_junk(self):
        with self.assertRaises(ValueError):
            Engine.Engine(self.bad_id)
        with self.assertRaises(ValueError):
            Engine.Engine()

    def test_by_id_openssl(self):
        Engine.load_openssl()
        e = Engine.Engine('openssl')
        self.assertEqual(e.get_name(), 'Software engine support')
        self.assertEqual(e.get_id(), 'openssl')

    def test_by_id_dynamic(self):
        Engine.load_dynamic()
        Engine.Engine('dynamic')

    def test_engine_ctrl_cmd_string(self):
        Engine.load_dynamic()
        e = Engine.Engine('dynamic')
        e.ctrl_cmd_string('ID', 'TESTID')

    def test_load_private(self):
        Engine.load_openssl()
        e = Engine.Engine('openssl')
        e.set_default()
        e.load_private_key(self.privkey)

    def test_load_certificate(self):
        Engine.load_openssl()
        e = Engine.Engine('openssl')
        e.set_default()
        try:
            with self.assertRaises(Engine.EngineError):
                e.load_certificate('/dev/null')
        except SystemError:
            pass


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(EngineTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner().run(suite())
