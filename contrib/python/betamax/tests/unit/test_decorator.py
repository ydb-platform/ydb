try:
    from unittest import mock
except ImportError:
    import mock

import betamax
from betamax.decorator import use_cassette


@mock.patch('betamax.recorder.Betamax', autospec=True)
def test_wraps_session(Betamax):
    # This needs to be a magic mock so it will mock __exit__
    recorder = mock.MagicMock(spec=betamax.Betamax)
    recorder.use_cassette.return_value = recorder
    Betamax.return_value = recorder

    @use_cassette('foo', cassette_library_dir='fizbarbogus')
    def _test(session):
        pass

    _test()
    Betamax.assert_called_once_with(
        session=mock.ANY,
        cassette_library_dir='fizbarbogus',
        default_cassette_options={}
    )
    recorder.use_cassette.assert_called_once_with('foo')


@mock.patch('betamax.recorder.Betamax', autospec=True)
@mock.patch('requests.Session')
def test_creates_a_new_session(Session, Betamax):
    @use_cassette('foo', cassette_library_dir='dir')
    def _test(session):
        pass

    _test()

    assert Session.call_count == 1
