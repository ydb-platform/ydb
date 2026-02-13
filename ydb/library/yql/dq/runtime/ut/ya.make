UNITTEST_FOR(ydb/library/yql/dq/runtime)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    dq_arrow_helpers_ut.cpp
    dq_channel_service_ut.cpp
    dq_output_channel_ut.cpp
    ut_helper.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/threading/local_executor
    ydb/core/kqp/ut/common
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
