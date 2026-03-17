import betamax

from . import helper


def prerecord_hook(interaction, cassette):
    assert cassette.interactions == []
    interaction.data['response']['headers']['Betamax-Fake-Header'] = 'success'


def ignoring_hook(interaction, cassette):
    interaction.ignore()


def preplayback_hook(interaction, cassette):
    assert cassette.interactions != []
    interaction.data['response']['headers']['Betamax-Fake-Header'] = 'temp'


class Counter(object):
    def __init__(self):
        self.value = 0

    def increment(self, cassette):
        self.value += 1


class TestHooks(helper.IntegrationHelper):
    def tearDown(self):
        super(TestHooks, self).tearDown()
        # Clear out the hooks
        betamax.configure.Configuration.recording_hooks.pop('after_start', None)
        betamax.cassette.Cassette.hooks.pop('before_record', None)
        betamax.cassette.Cassette.hooks.pop('before_playback', None)
        betamax.configure.Configuration.recording_hooks.pop('before_stop', None)

    def test_post_start_hook(self):
        start_count = Counter()
        with betamax.Betamax.configure() as config:
            config.after_start(callback=start_count.increment)

        recorder = betamax.Betamax(self.session)

        assert start_count.value == 0
        with recorder.use_cassette('after_start_hook'):
            assert start_count.value == 1
            self.cassette_path = recorder.current_cassette.cassette_path
            self.session.get('https://httpbin.org/get')

        assert start_count.value == 1
        with recorder.use_cassette('after_start_hook', record='none'):
            assert start_count.value == 2
            self.session.get('https://httpbin.org/get')
        assert start_count.value == 2

    def test_pre_stop_hook(self):
        stop_count = Counter()
        with betamax.Betamax.configure() as config:
            config.before_stop(callback=stop_count.increment)

        recorder = betamax.Betamax(self.session)

        assert stop_count.value == 0
        with recorder.use_cassette('before_stop_hook'):
            self.cassette_path = recorder.current_cassette.cassette_path
            self.session.get('https://httpbin.org/get')
            assert stop_count.value == 0
        assert stop_count.value == 1

        with recorder.use_cassette('before_stop_hook', record='none'):
            self.session.get('https://httpbin.org/get')
            assert stop_count.value == 1
        assert stop_count.value == 2

    def test_prerecord_hook(self):
        with betamax.Betamax.configure() as config:
            config.before_record(callback=prerecord_hook)

        recorder = betamax.Betamax(self.session)
        with recorder.use_cassette('prerecord_hook'):
            self.cassette_path = recorder.current_cassette.cassette_path
            response = self.session.get('https://httpbin.org/get')
            assert response.headers['Betamax-Fake-Header'] == 'success'

        with recorder.use_cassette('prerecord_hook', record='none'):
            response = self.session.get('https://httpbin.org/get')
            assert response.headers['Betamax-Fake-Header'] == 'success'

    def test_preplayback_hook(self):
        with betamax.Betamax.configure() as config:
            config.before_playback(callback=preplayback_hook)

        recorder = betamax.Betamax(self.session)
        with recorder.use_cassette('preplayback_hook'):
            self.cassette_path = recorder.current_cassette.cassette_path
            self.session.get('https://httpbin.org/get')

        with recorder.use_cassette('preplayback_hook', record='none'):
            response = self.session.get('https://httpbin.org/get')
            assert response.headers['Betamax-Fake-Header'] == 'temp'

    def test_prerecord_ignoring_hook(self):
        with betamax.Betamax.configure() as config:
            config.before_record(callback=ignoring_hook)

        recorder = betamax.Betamax(self.session)
        with recorder.use_cassette('ignore_hook'):
            self.cassette_path = recorder.current_cassette.cassette_path
            self.session.get('https://httpbin.org/get')
            assert recorder.current_cassette.interactions == []
