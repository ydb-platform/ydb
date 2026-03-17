PY3TEST()

FORK_SUBTESTS()

PEERDIR(
    contrib/python/PyNaCl/py3
    contrib/python/hypothesis
)

NO_LINT()

DATA(arcadia/contrib/python/PyNaCl/py3/tests/data)

TEST_SRCS(
    __init__.py
    test_aead.py
    test_bindings.py
    test_box.py
    test_encoding.py
    test_exc.py
    test_generichash.py
    test_hash.py
    test_hashlib_scrypt.py
    test_kx.py
    test_public.py
    test_pwhash.py
    test_sealed_box.py
    test_secret.py
    test_secretstream.py
    test_shorthash.py
    test_signing.py
    test_utils.py
    utils.py
)

END()
