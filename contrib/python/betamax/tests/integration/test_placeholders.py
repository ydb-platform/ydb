from betamax import Betamax
from betamax.cassette import Cassette

from copy import deepcopy
from tests.integration.helper import IntegrationHelper

original_cassette_options = deepcopy(Cassette.default_cassette_options)
b64_foobar = 'Zm9vOmJhcg=='  # base64.b64encode('foo:bar')


class TestPlaceholders(IntegrationHelper):
    def setUp(self):
        super(TestPlaceholders, self).setUp()
        config = Betamax.configure()
        config.define_cassette_placeholder('<AUTHORIZATION>', b64_foobar)

    def tearDown(self):
        super(TestPlaceholders, self).tearDown()
        Cassette.default_cassette_options = original_cassette_options

    def test_placeholders_work(self):
        placeholders = Cassette.default_cassette_options['placeholders']
        assert placeholders == [{
            'placeholder': '<AUTHORIZATION>',
            'replace': b64_foobar,
        }]

        s = self.session
        cassette = None
        with Betamax(s).use_cassette('test_placeholders') as recorder:
            r = s.get('http://httpbin.org/get', auth=('foo', 'bar'))
            cassette = recorder.current_cassette
            self.cassette_path = cassette.cassette_path
            assert r.status_code == 200
            auth = r.json()['headers']['Authorization']
            assert b64_foobar in auth

        self.cassette_path = cassette.cassette_path
        i = cassette.interactions[0]
        auth = i.data['request']['headers']['Authorization']
        assert '<AUTHORIZATION>' in auth[0]
