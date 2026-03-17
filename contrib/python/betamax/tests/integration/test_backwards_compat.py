import betamax
import copy
from .helper import IntegrationHelper


class TestBackwardsCompatibleSerialization(IntegrationHelper):
    def setUp(self):
        super(TestBackwardsCompatibleSerialization, self).setUp()
        self.cassette_created = False
        opts = betamax.cassette.Cassette.default_cassette_options
        self.original_defaults = copy.deepcopy(opts)

        with betamax.Betamax.configure() as config:
            config.define_cassette_placeholder('<FOO>', 'nothing to replace')

    def tearDown(self):
        super(TestBackwardsCompatibleSerialization, self).setUp()
        Cassette = betamax.cassette.Cassette
        Cassette.default_cassette_options = self.original_defaults

    def test_can_deserialize_an_old_cassette(self):
        with betamax.Betamax(self.session).use_cassette('GitHub_emojis') as b:
            assert b.current_cassette is not None
            cassette = b.current_cassette
            assert len(cassette.interactions) > -1

    def test_matches_old_request_data(self):
        with betamax.Betamax(self.session).use_cassette('GitHub_emojis'):
            r = self.session.get('https://api.github.com/emojis')
            assert r is not None

    def tests_populates_correct_fields_with_missing_data(self):
        with betamax.Betamax(self.session).use_cassette('GitHub_emojis'):
            r = self.session.get('https://api.github.com/emojis')
            assert r.reason == 'OK'
            assert r.status_code == 200

    def tests_deserializes_old_cassette_headers(self):
        with betamax.Betamax(self.session).use_cassette('GitHub_emojis') as b:
            self.session.get('https://api.github.com/emojis')
            interaction = b.current_cassette.interactions[0].data
            header = interaction['request']['headers']['Accept']
            assert not isinstance(header, list)
