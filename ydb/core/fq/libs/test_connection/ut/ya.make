UNITTEST_FOR(ydb/core/fq/libs/control_plane_proxy)

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    test_connection_ut.cpp
)

END()
