import pytest
import unittest

from betamax import Betamax, BetamaxError
from requests import Session


class TestOncePreventsNewInteractions(unittest.TestCase):

    """Test that using a cassette with once record mode prevents new requests.

    """

    def test_once_prevents_new_requests(self):
        s = Session()
        with Betamax(s).use_cassette('once_record_mode'):
            with pytest.raises(BetamaxError):
                s.get('http://example.com')
