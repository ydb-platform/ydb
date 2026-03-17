PY3TEST()

PEERDIR(
    contrib/python/argon2-cffi-bindings
)

TEST_SRCS(
    #test_build.py
    test_smoke.py
)

NO_LINT()

END()
