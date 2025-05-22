PY2TEST()

SUBSCRIBER(g:python-contrib)

PEERDIR(
    contrib/deprecated/python/ruamel.ordereddict
)

TEST_SRCS(
    test_ordereddict.py
    test_py2.py
    test_py27.py
)

NO_LINT()

END()
