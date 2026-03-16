PY3TEST()

PEERDIR(
    contrib/python/grafanalib
)

TEST_SRCS(
    test_core.py
    test_grafanalib.py
    test_opentsdb.py
    test_validators.py
    test_zabbix.py
)

END()
