import os
import unittest

from requests import Session


class IntegrationHelper(unittest.TestCase):
    cassette_created = True

    def setUp(self):
        self.cassette_path = None
        self.session = Session()

    def tearDown(self):
        if self.cassette_created:
            assert self.cassette_path is not None
            os.unlink(self.cassette_path)
