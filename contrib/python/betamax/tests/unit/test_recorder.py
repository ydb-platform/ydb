import copy
import unittest

from betamax import matchers, serializers
from betamax.adapter import BetamaxAdapter
from betamax.cassette import Cassette, cassette
from betamax.recorder import Betamax
from requests import Session
from requests.adapters import HTTPAdapter


class TestBetamax(unittest.TestCase):
    def setUp(self):
        self.session = Session()
        self.vcr = Betamax(self.session)
        self.cassette_options = copy.deepcopy(
            Cassette.default_cassette_options
        )

    def tearDown(self):
        self.session = Session()
        self.vcr = Betamax(self.session)
        Cassette.default_cassette_options = self.cassette_options

    def test_initialization_does_not_alter_the_session(self):
        for v in self.session.adapters.values():
            assert not isinstance(v, BetamaxAdapter)
            assert isinstance(v, HTTPAdapter)

    def test_initialization_converts_placeholders(self):
        placeholders = [{'placeholder': '<FOO>', 'replace': 'replace-with'}]
        default_cassette_options = {'placeholders': placeholders}
        self.vcr = Betamax(self.session,
                           default_cassette_options=default_cassette_options)
        assert self.vcr.config.default_cassette_options['placeholders'] == [{
            'placeholder': '<FOO>',
            'replace': 'replace-with',
        }]

    def test_entering_context_alters_adapters(self):
        with self.vcr:
            for v in self.session.adapters.values():
                assert isinstance(v, BetamaxAdapter)

    def test_exiting_resets_the_adapters(self):
        with self.vcr:
            pass
        for v in self.session.adapters.values():
            assert not isinstance(v, BetamaxAdapter)

    def test_current_cassette(self):
        assert self.vcr.current_cassette is None
        self.vcr.use_cassette('test')
        assert isinstance(self.vcr.current_cassette, cassette.Cassette)

    def test_use_cassette_returns_cassette_object(self):
        assert self.vcr.use_cassette('test') is self.vcr

    def test_register_request_matcher(self):
        class FakeMatcher(object):
            name = 'fake_matcher'

        Betamax.register_request_matcher(FakeMatcher)
        assert 'fake_matcher' in matchers.matcher_registry
        assert isinstance(matchers.matcher_registry['fake_matcher'],
                          FakeMatcher)

    def test_register_serializer(self):
        class FakeSerializer(object):
            name = 'fake_serializer'

        Betamax.register_serializer(FakeSerializer)
        assert 'fake_serializer' in serializers.serializer_registry
        assert isinstance(serializers.serializer_registry['fake_serializer'],
                          FakeSerializer)

    def test_stores_the_session_instance(self):
        assert self.session is self.vcr.session

    def test_use_cassette_passes_along_placeholders(self):
        placeholders = [{'placeholder': '<FOO>', 'replace': 'replace-with'}]
        self.vcr.use_cassette('test', placeholders=placeholders)
        assert self.vcr.current_cassette.placeholders == [
            cassette.Placeholder.from_dict(p) for p in placeholders
        ]
