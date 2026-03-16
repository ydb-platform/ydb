PY3TEST()

PEERDIR(
    contrib/python/mysqlclient
)

TEST_SRCS(
    test_MySQLdb_times.py
    test__mysql.py
)

PY_SRCS(
    TOP_LEVEL
    capabilities.py
    configdb.py
    dbapi20.py
)

NO_LINT()

END()
