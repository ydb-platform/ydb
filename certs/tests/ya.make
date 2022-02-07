PY2TEST()

OWNER(
    g:util
    g:juggler
)

TEST_SRCS(test_fetch.py)

TAG(ya:external)

REQUIREMENTS(network:full)

PEERDIR(
    library/python/resource
    certs
)

END()
