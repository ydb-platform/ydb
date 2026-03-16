PY3TEST()

PEERDIR(
    contrib/python/acme
    contrib/python/pytest
    contrib/python/pytest-xdist
    contrib/python/typing-extensions
)

SRCDIR(contrib/python/acme/acme/_internal/tests)

RESOURCE_FILES(
    STRIP contrib/python/acme/acme/_internal/tests/
    contrib/python/acme/acme/_internal/tests/testdata/cert-100sans.pem
    contrib/python/acme/acme/_internal/tests/testdata/cert.der
    contrib/python/acme/acme/_internal/tests/testdata/cert-idnsans.pem
    contrib/python/acme/acme/_internal/tests/testdata/cert-ipsans.pem
    contrib/python/acme/acme/_internal/tests/testdata/cert-ipv6sans.pem
    contrib/python/acme/acme/_internal/tests/testdata/cert-nocn.der
    contrib/python/acme/acme/_internal/tests/testdata/cert.pem
    contrib/python/acme/acme/_internal/tests/testdata/cert-san.pem
    contrib/python/acme/acme/_internal/tests/testdata/critical-san.pem
    contrib/python/acme/acme/_internal/tests/testdata/csr-100sans.pem
    contrib/python/acme/acme/_internal/tests/testdata/csr-6sans.pem
    contrib/python/acme/acme/_internal/tests/testdata/csr.der
    contrib/python/acme/acme/_internal/tests/testdata/csr-idnsans.pem
    contrib/python/acme/acme/_internal/tests/testdata/csr-ipsans.pem
    contrib/python/acme/acme/_internal/tests/testdata/csr-ipv6sans.pem
    contrib/python/acme/acme/_internal/tests/testdata/csr-mixed.pem
    contrib/python/acme/acme/_internal/tests/testdata/csr-nosans.pem
    contrib/python/acme/acme/_internal/tests/testdata/csr.pem
    contrib/python/acme/acme/_internal/tests/testdata/csr-san.pem
    contrib/python/acme/acme/_internal/tests/testdata/dsa512_key.pem
    contrib/python/acme/acme/_internal/tests/testdata/ec_secp384r1_key.pem
    contrib/python/acme/acme/_internal/tests/testdata/rsa1024_cert.pem
    contrib/python/acme/acme/_internal/tests/testdata/rsa1024_key.pem
    contrib/python/acme/acme/_internal/tests/testdata/rsa2048_cert.pem
    contrib/python/acme/acme/_internal/tests/testdata/rsa2048_key.pem
    contrib/python/acme/acme/_internal/tests/testdata/rsa256_key.pem
    contrib/python/acme/acme/_internal/tests/testdata/rsa4096_cert.pem
    contrib/python/acme/acme/_internal/tests/testdata/rsa4096_key.pem
    contrib/python/acme/acme/_internal/tests/testdata/rsa512_key.pem
)

TEST_SRCS(
    __init__.py
    challenges_test.py
    client_test.py
    crypto_util_test.py
    errors_test.py
    fields_test.py
    jose_test.py
    jws_test.py
    messages_test.py
    standalone_test.py
    test_util.py
    util_test.py
)

NO_LINT()

END()
