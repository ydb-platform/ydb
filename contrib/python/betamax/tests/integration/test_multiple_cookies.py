import betamax

from .helper import IntegrationHelper


class TestMultipleCookies(IntegrationHelper):
    """Previously our handling of multiple instances of cookies was wrong.

    This set of tests is here to ensure that we properly serialize/deserialize
    the case where the client receives and betamax serializes multiple
    Set-Cookie headers.

    See the following for more information:

    - https://github.com/sigmavirus24/betamax/pull/60
    - https://github.com/sigmavirus24/betamax/pull/59
    - https://github.com/sigmavirus24/betamax/issues/58
    """
    def setUp(self):
        super(TestMultipleCookies, self).setUp()
        self.cassette_created = False

    def test_multiple_cookies(self):
        """Make a request to httpbin.org and verify we serialize it correctly.

        We should be able to see that the cookiejar on the session has the
        cookies properly parsed and distinguished.
        """
        recorder = betamax.Betamax(self.session)
        cassette_name = 'test-multiple-cookies-regression'
        url = 'https://httpbin.org/cookies/set'
        cookies = {
            'cookie0': 'value0',
            'cookie1': 'value1',
            'cookie2': 'value2',
            'cookie3': 'value3',
        }
        with recorder.use_cassette(cassette_name):
            self.session.get(url, params=cookies)

        for name, value in cookies.items():
            assert self.session.cookies[name] == value
