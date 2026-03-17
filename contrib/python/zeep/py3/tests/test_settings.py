from zeep.settings import Settings


def test_settings_set_context_raw_response():
    settings = Settings()

    assert settings.raw_response is False
    with settings(raw_response=True):
        assert settings.raw_response is True

        with settings():
            # Check that raw_response is not changed by default value
            assert settings.raw_response is True
    # Check that the original value returned
    assert settings.raw_response is False


def test_settings_set_context_with_exception():
    settings = Settings()

    assert settings.raw_response is False
    try:
        with settings(raw_response=True):
            assert settings.raw_response is True
            raise RuntimeError
    except RuntimeError:
        pass
    # Check that the original value returned
    assert settings.raw_response is False
