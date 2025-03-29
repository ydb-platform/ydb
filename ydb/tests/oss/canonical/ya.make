PY3_LIBRARY()

PY_SRCS(
    __init__.py
    conftest.py
    canonical.py
)

PEERDIR(
    library/python/testing/yatest_common
)

END()
