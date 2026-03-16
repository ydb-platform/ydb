PY3TEST()

PEERDIR(
    contrib/python/distro
)

DATA(
    arcadia/contrib/python/distro/py3/tests/resources
)

DEPENDS(
    contrib/python/distro/py3/bin
)

TEST_SRCS(
    test_distro.py
)

NO_LINT()

END()
