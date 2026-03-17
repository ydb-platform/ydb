import os
import subprocess
import sys
import unittest

from ijson import compat
from test.test_base import JSON


class DumpTests(unittest.TestCase):

    def _do_test_dump(self, method, multiple_values):
        # Use python backend to ensure multiple_values works
        env = dict(os.environ)
        env['IJSON_BACKEND'] = 'python'
        # Ensure printing works on the subprocess in Windows
        # by using utf-8 on its stdout
        if 'win' in sys.platform:
            env = dict(os.environ)
            env['PYTHONIOENCODING'] = 'utf-8'
        cmd = [sys.executable, '-m', 'ijson.dump', '-m', method, '-p', '']
        if multiple_values:
            cmd.append('-M')
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        input_data = JSON
        if multiple_values:
            input_data += JSON
        out, err = proc.communicate(input_data)
        status = proc.wait()
        self.assertEqual(0, status, "out:\n%s\nerr:%s" % (compat.b2s(out), compat.b2s(err)))

    def _test_dump(self, method):
        self._do_test_dump(method, True)
        self._do_test_dump(method, False)

    def test_basic_parse(self):
        self._test_dump('basic_parse')

    def test_parse(self):
        self._test_dump('parse')

    def test_kvitems(self):
        self._test_dump('kvitems')

    def test_items(self):
        self._test_dump('items')