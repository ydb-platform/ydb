def test_version():
    from pycrfsuite import CRFSUITE_VERSION

    assert bool(CRFSUITE_VERSION), CRFSUITE_VERSION
