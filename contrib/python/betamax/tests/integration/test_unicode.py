from betamax import Betamax
from tests.integration.helper import IntegrationHelper


class TestUnicode(IntegrationHelper):
    def test_unicode_is_saved_properly(self):
        s = self.session
        # https://github.com/kanzure/python-requestions/issues/4
        url = 'http://www.amazon.com/review/RAYTXRF3122TO'

        with Betamax(s).use_cassette('test_unicode') as beta:
            self.cassette_path = beta.current_cassette.cassette_path
            s.get(url)
