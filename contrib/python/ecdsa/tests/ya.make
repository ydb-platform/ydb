PY3TEST()

PEERDIR(
    contrib/python/ecdsa
    contrib/python/hypothesis
)

SRCDIR(
    contrib/python/ecdsa/ecdsa
)

TEST_SRCS(
    test_curves.py
    test_der.py
    test_ecdh.py
    test_ecdsa.py
    test_eddsa.py
    test_ellipticcurve.py
    test_jacobi.py
    test_keys.py
    test_malformed_sigs.py
    test_numbertheory.py
    test_pyecdsa.py
    test_rw_lock.py
    test_sha3.py
)

NO_LINT()

END()
