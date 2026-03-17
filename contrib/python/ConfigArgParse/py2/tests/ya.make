PY2TEST()

PEERDIR(
    contrib/python/ConfigArgParse
    contrib/python/mock
)

TEST_SRCS(
    test_configargparse.py
)

NO_LINT()

END()
