import yarl

# Don't check the actual behavior but make sure that calls are allowed


def teardown_module():
    yarl.cache_configure()


def test_cache_clear() -> None:
    yarl.cache_clear()


def test_cache_info() -> None:
    info = yarl.cache_info()
    assert info.keys() == {"idna_encode", "idna_decode"}


def test_cache_configure_default() -> None:
    yarl.cache_configure()


def test_cache_configure_None() -> None:
    yarl.cache_configure(idna_encode_size=None, idna_decode_size=None)


def test_cache_configure_explicit() -> None:
    yarl.cache_configure(idna_encode_size=128, idna_decode_size=128)
