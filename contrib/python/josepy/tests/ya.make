PY3TEST()

PEERDIR(
    contrib/python/josepy
)

RESOURCE_FILES(
    testdata/cert-100sans.pem
    testdata/cert-idnsans.pem
    testdata/cert-san.pem
    testdata/cert.der
    testdata/cert.pem
    testdata/critical-san.pem
    testdata/csr-100sans.pem
    testdata/csr-6sans.pem
    testdata/csr-idnsans.pem
    testdata/csr-nosans.pem
    testdata/csr-san.pem
    testdata/csr.der
    testdata/csr.pem
    testdata/dsa512_key.pem
    testdata/ec_p256_key.pem
    testdata/ec_p384_key.pem
    testdata/ec_p521_key.pem
    testdata/rsa1024_key.pem
    testdata/rsa2048_cert.pem
    testdata/rsa2048_key.pem
    testdata/rsa256_key.pem
    testdata/rsa512_key.pem
)

TEST_SRCS(
    b64_test.py
    errors_test.py
    init_test.py
    interfaces_test.py
    json_util_test.py
    jwa_test.py
    jwk_test.py
    jws_test.py
    magic_typing_test.py
    test_util.py
    util_test.py
)

NO_LINT()

END()
