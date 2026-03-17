# pylint: disable=missing-module-docstring,missing-function-docstring
import mmh3


def test_function_docstrings() -> None:
    assert "__doc__" in dir(mmh3.hash)
    assert mmh3.hash.__doc__ is not None
    assert mmh3.hash.__doc__.startswith("hash(key, seed=0, signed=True) -> int\n\n")

    assert "__doc__" in dir(mmh3.hash_from_buffer)
    assert mmh3.hash_from_buffer.__doc__ is not None
    assert mmh3.hash_from_buffer.__doc__.startswith(
        "hash_from_buffer(key, seed=0, signed=True) -> int\n\n"
    )

    assert "__doc__" in dir(mmh3.hash64)
    assert mmh3.hash64.__doc__ is not None
    assert mmh3.hash64.__doc__.startswith(
        "hash64(key, seed=0, x64arch=True, signed=True) -> tuple[int, int]\n\n"
    )

    assert "__doc__" in dir(mmh3.hash128)
    assert mmh3.hash128.__doc__ is not None
    assert mmh3.hash128.__doc__.startswith(
        "hash128(key, seed=0, x64arch=True, signed=False) -> int\n\n"
    )

    assert "__doc__" in dir(mmh3.hash_bytes)
    assert mmh3.hash_bytes.__doc__ is not None
    assert mmh3.hash_bytes.__doc__.startswith(
        "hash_bytes(key, seed=0, x64arch=True) -> bytes\n\n"
    )


def test_module_docstring() -> None:
    assert "__doc__" in dir(mmh3)
    assert mmh3.__doc__ is not None
    assert mmh3.__doc__.startswith("A Python front-end to MurmurHash3")
