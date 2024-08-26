UNITTEST_FOR(ydb/core/kqp/provider)

SRCS(
    yql_kikimr_gateway_ut.cpp
    yql_kikimr_provider_ut.cpp
    read_attributes_utils_ut.cpp
)

PEERDIR(
    ydb/core/client/minikql_result_lib
    ydb/core/kqp/ut/common
    ydb/library/yql/sql/pg_dummy
    library/cpp/testing/gmock_in_unittest
)

YQL_LAST_ABI_VERSION()

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

END()
