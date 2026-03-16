PY3_LIBRARY()

LICENSE(
    Apache-2.0 AND
    LicenseRef-scancode-unknown-license-reference AND
    MIT
)

WITHOUT_LICENSE_TEXTS()

VERSION(3.3.0)

PEERDIR(
    contrib/python/chdb/cpp/programs
    contrib/python/chdb/py3
)

END()

RECURSE(
    cpp
    py3
)

RECURSE_FOR_TESTS(
    py3/tests
)
