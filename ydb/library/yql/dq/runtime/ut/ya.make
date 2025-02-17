UNITTEST_FOR(ydb/library/yql/dq/runtime)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    dq_arrow_helpers_ut.cpp
    dq_output_channel_ut.cpp
    ut_helper.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
