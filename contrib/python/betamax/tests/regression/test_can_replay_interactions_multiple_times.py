import unittest

from betamax import Betamax
from requests import Session


class TestReplayInteractionMultipleTimes(unittest.TestCase):
    """
    Test that an Interaction can be replayed multiple times within the same
    betamax session.
    """
    def test_replay_interaction_more_than_once(self):
        s = Session()

        with Betamax(s).use_cassette('replay_multiple_times', record='once',
                                     allow_playback_repeats=True):
            for k in range(1, 5):
                r = s.get('http://httpbin.org/stream/3', stream=True)
                assert r.raw.read(1028), "Stream already consumed. Try: %d" % k
