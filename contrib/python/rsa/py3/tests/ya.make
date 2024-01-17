PY3TEST()

PEERDIR(
    contrib/python/rsa
)

NO_LINT()

TEST_SRCS(
    test_cli.py
    test_common.py
    test_integers.py
    test_key.py
    test_load_save_keys.py
    test_parallel.py
    test_pem.py
    test_pkcs1.py
    test_pkcs1_v2.py
    test_prime.py
    test_strings.py
    test_transform.py
)

DATA (
    arcadia/contrib/python/rsa/py3/tests
)

END()
