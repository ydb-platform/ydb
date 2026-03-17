import betamax

from tests.integration import helper


class TestPlaybackRepeatInteractions(helper.IntegrationHelper):
    def test_will_replay_the_same_interaction(self):
        self.cassette_created = False
        s = self.session
        recorder = betamax.Betamax(s)
        # NOTE(sigmavirus24): Ensure the cassette is recorded
        with recorder.use_cassette('replay_interactions'):
            cassette = recorder.current_cassette
            r = s.get('http://httpbin.org/get')
            assert r.status_code == 200
            assert len(cassette.interactions) == 1

        with recorder.use_cassette('replay_interactions',
                                   allow_playback_repeats=True):
            cassette = recorder.current_cassette
            r = s.get('http://httpbin.org/get')
            assert r.status_code == 200
            assert len(cassette.interactions) == 1
            r = s.get('http://httpbin.org/get')
            assert r.status_code == 200
            assert len(cassette.interactions) == 1
            assert cassette.interactions[0].used is False
