import pytest

import yarl

# Don't check the actual behavior but make sure that calls are allowed


def teardown_module() -> None:
    yarl.cache_configure()


def test_cache_clear() -> None:
    yarl.cache_clear()


def test_cache_info() -> None:
    info = yarl.cache_info()
    assert info.keys() == {
        "idna_encode",
        "idna_decode",
        "ip_address",
        "host_validate",
        "encode_host",
    }


def test_cache_configure_default() -> None:
    yarl.cache_configure()


def test_cache_configure_None() -> None:
    yarl.cache_configure(
        idna_decode_size=None,
        idna_encode_size=None,
        encode_host_size=None,
    )


def test_cache_configure_None_including_deprecated() -> None:
    msg = (
        r"cache_configure\(\) no longer accepts the ip_address_size "
        r"or host_validate_size arguments, they are used to set the "
        r"encode_host_size instead and will be removed in the future"
    )
    with pytest.warns(DeprecationWarning, match=msg):
        yarl.cache_configure(
            idna_decode_size=None,
            idna_encode_size=None,
            encode_host_size=None,
            ip_address_size=None,
            host_validate_size=None,
        )
    assert yarl.cache_info()["idna_decode"].maxsize is None
    assert yarl.cache_info()["idna_encode"].maxsize is None
    assert yarl.cache_info()["encode_host"].maxsize is None


def test_cache_configure_None_only_deprecated() -> None:
    msg = (
        r"cache_configure\(\) no longer accepts the ip_address_size "
        r"or host_validate_size arguments, they are used to set the "
        r"encode_host_size instead and will be removed in the future"
    )
    with pytest.warns(DeprecationWarning, match=msg):
        yarl.cache_configure(
            ip_address_size=None,
            host_validate_size=None,
        )
    assert yarl.cache_info()["encode_host"].maxsize is None


def test_cache_configure_explicit() -> None:
    yarl.cache_configure(
        idna_decode_size=128,
        idna_encode_size=128,
        encode_host_size=128,
    )
    assert yarl.cache_info()["idna_decode"].maxsize == 128
    assert yarl.cache_info()["idna_encode"].maxsize == 128
    assert yarl.cache_info()["encode_host"].maxsize == 128


def test_cache_configure_waring() -> None:
    msg = (
        r"cache_configure\(\) no longer accepts the ip_address_size "
        r"or host_validate_size arguments, they are used to set the "
        r"encode_host_size instead and will be removed in the future"
    )
    with pytest.warns(DeprecationWarning, match=msg):
        yarl.cache_configure(
            idna_encode_size=1024,
            idna_decode_size=1024,
            ip_address_size=1024,
            host_validate_size=1024,
        )

    assert yarl.cache_info()["encode_host"].maxsize == 1024
    with pytest.warns(DeprecationWarning, match=msg):
        yarl.cache_configure(host_validate_size=None)

    assert yarl.cache_info()["encode_host"].maxsize is None
