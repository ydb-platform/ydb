PY3TEST()

PEERDIR(
    contrib/python/affine
)

NO_LINT()

SRCDIR(
    contrib/python/affine
)

TEST_SRCS(
    affine/tests/__init__.py
    affine/tests/test_pickle.py
    affine/tests/test_rotation.py
    affine/tests/test_serialize.py
    affine/tests/test_transform.py
)

END()
