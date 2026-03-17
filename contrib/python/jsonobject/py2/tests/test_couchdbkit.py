from __future__ import absolute_import
from __future__ import unicode_literals
import json
import os
from unittest2 import TestCase
from .couchdbkit.application import Application
from io import open


class CouchdbkitTestCase(TestCase):
    def _test(self, name):
        with open(os.path.join('test', 'couchdbkit', 'data', '{0}.json'.format(name))) as f:
            Application.wrap(json.load(f))

    def test_basic(self):
        self._test('basic')

    def test_medium(self):
        self._test('medium')

    def test_large(self):
        self._test('large')

    def test_multimedia_map(self):
        self._test('multimedia_map')
