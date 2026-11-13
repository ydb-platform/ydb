# Native Rust build; the Python API remains cryptography 41.0.6.

PY3_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(41.0.6)

ORIGINAL_SOURCE(https://github.com/pyca/cryptography/archive/refs/tags/41.0.6.tar.gz)

PEERDIR(
    contrib/libs/openssl
    contrib/python/cryptography/next/rust/src/rust
)

SRCS(_openssl.c)

# Match upstream cryptography-cffi: CPython's stable ABI starts at 3.7.
CFLAGS(-DPy_LIMITED_API=0x030700f0)

PY_REGISTER(
    _rust
    _openssl
)

NO_COMPILER_WARNINGS()

NO_UTIL()

END()
