PY2TEST()

PEERDIR(
    contrib/python/mock
    contrib/python/mysqlclient
)

TEST_SRCS(
    test__mysql.py
    test_MySQLdb_times.py
)

PY_SRCS(
    TOP_LEVEL
    capabilities.py
    configdb.py
    dbapi20.py
)

NO_LINT()

END()
