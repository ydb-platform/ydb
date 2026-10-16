UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/base
    ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

SRCS(
    ut_path_footprint.cpp
)

YQL_LAST_ABI_VERSION()

END()
