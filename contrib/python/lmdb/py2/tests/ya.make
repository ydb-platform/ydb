PY2TEST()

LICENSE(OLDAP-2.8)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(1.4.1)

PEERDIR(
    contrib/python/lmdb/py2
)

NO_LINT()

PY_SRCS(
    TOP_LEVEL
    testlib.py
)

TEST_SRCS(
    crash_test.py
    cursor_test.py
    env_test.py
    getmulti_test.py
    iteration_test.py
    package_test.py
    tool_test.py
    txn_test.py
)

END()
