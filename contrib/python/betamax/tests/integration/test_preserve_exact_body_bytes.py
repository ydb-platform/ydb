from .helper import IntegrationHelper
from betamax import Betamax
from betamax.cassette import Cassette

import copy


class TestPreserveExactBodyBytes(IntegrationHelper):
    def test_preserve_exact_body_bytes_does_not_munge_response_content(self):
        # Do not delete this cassette after the test
        self.cassette_created = False

        with Betamax(self.session) as b:
            b.use_cassette('preserve_exact_bytes',
                           preserve_exact_body_bytes=True,
                           match_requests_on=['uri', 'method', 'body'])
            r = self.session.post('https://httpbin.org/post',
                                  data={'a': 1})
            assert 'headers' in r.json()

            interaction = b.current_cassette.interactions[0].data
            assert 'base64_string' in interaction['request']['body']
            assert 'base64_string' in interaction['response']['body']


class TestPreserveExactBodyBytesForAllCassettes(IntegrationHelper):
    def setUp(self):
        super(TestPreserveExactBodyBytesForAllCassettes, self).setUp()
        self.orig = copy.deepcopy(Cassette.default_cassette_options)
        self.cassette_created = False

    def tearDown(self):
        super(TestPreserveExactBodyBytesForAllCassettes, self).tearDown()
        Cassette.default_cassette_options = self.orig

    def test_preserve_exact_body_bytes(self):
        with Betamax.configure() as config:
            config.preserve_exact_body_bytes = True

        with Betamax(self.session) as b:
            b.use_cassette('global_preserve_exact_body_bytes')
            r = self.session.get('https://httpbin.org/get')
            assert 'headers' in r.json()

            interaction = b.current_cassette.interactions[0].data
            assert 'base64_string' in interaction['response']['body']
