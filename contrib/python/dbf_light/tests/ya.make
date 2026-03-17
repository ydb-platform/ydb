PY3TEST()

PEERDIR(
    contrib/python/dbf_light
)

DATA(
    arcadia/contrib/python/dbf_light/tests
)

TEST_SRCS(
    test_module.py
)

NO_LINT()

END()
