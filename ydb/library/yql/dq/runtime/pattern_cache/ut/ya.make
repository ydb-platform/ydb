UNITTEST_FOR(ydb/library/yql/dq/runtime/pattern_cache)

SIZE(SMALL)

SRCS(
    dq_pattern_cache_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
