PY3_LIBRARY()


ALL_PY_SRCS()

PEERDIR(
    contrib/python/docker
    contrib/python/xmltodict
    ydb/tests/library
)

END()

RECURSE_FOR_TESTS(
    ut
)