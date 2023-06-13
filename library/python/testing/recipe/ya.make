PY23_LIBRARY()

PY_SRCS(
    __init__.py
    ports.py
)

PEERDIR(
    contrib/python/ipdb
    library/python/testing/yatest_common
    library/python/testing/yatest_lib
)

END()
