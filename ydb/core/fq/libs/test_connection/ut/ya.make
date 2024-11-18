UNITTEST_FOR(ydb/core/fq/libs/control_plane_proxy)

PEERDIR(
    library/cpp/testing/unittest
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    test_connection_ut.cpp
)

END()
