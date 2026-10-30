PY3TEST()

PEERDIR(
    contrib/python/cryptography/next
    contrib/python/cryptography/next/tests/native
    contrib/python/temporalio
)

TEST_SRCS(test_native_rust.py)

END()
