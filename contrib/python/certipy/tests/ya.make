PY3TEST()

PEERDIR(
    contrib/python/certipy
    contrib/python/requests
    contrib/python/Flask
)

SRCDIR(contrib/python/certipy)

NO_LINT()

TEST_SRCS(
    certipy/test/__init__.py
    certipy/test/test_certipy.py
)

END()
