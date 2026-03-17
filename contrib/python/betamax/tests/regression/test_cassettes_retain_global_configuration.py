import pytest
import unittest

from betamax import Betamax, cassette
from requests import Session


class TestCassetteRecordMode(unittest.TestCase):
    def setUp(self):
        with Betamax.configure() as config:
            config.default_cassette_options['record_mode'] = 'none'

    def tearDown(self):
        with Betamax.configure() as config:
            config.default_cassette_options['record_mode'] = 'once'

    def test_record_mode_is_none(self):
        s = Session()
        with pytest.raises(ValueError):
            with Betamax(s) as recorder:
                recorder.use_cassette('regression_record_mode')
                assert recorder.current_cassette is None

    def test_class_variables_retain_their_value(self):
        opts = cassette.Cassette.default_cassette_options
        assert opts['record_mode'] == 'none'
