UNITTEST_FOR(ydb/core/kqp/common/result_set_format)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    kqp_formats_ut_helpers.cpp
    kqp_formats_arrow_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

END()
