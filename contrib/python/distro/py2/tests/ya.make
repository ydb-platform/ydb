PY2TEST()

PEERDIR(
    contrib/python/distro
)

DATA(
    arcadia/contrib/python/distro/py2/tests/resources
)

DEPENDS(
    contrib/python/distro/py2/bin
)

TEST_SRCS(
    test_distro.py
)

NO_LINT()

END()
