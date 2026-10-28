UNITTEST_FOR(ydb/library/yql/providers/native)

SRCS(read_actor_ut.cpp)
PEERDIR(
    library/cpp/testing/unittest
    ydb/library/yql/providers/common/ut_helpers
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)
YQL_LAST_ABI_VERSION()
END()
