UNITTEST_FOR(ydb/core/kqp/provider)

SRCS(
    yql_kikimr_gateway_ut.cpp
    yql_kikimr_provider_ut.cpp
    read_attributes_utils_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    yql/essentials/ast
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1
    library/cpp/testing/gmock_in_unittest
)

YQL_LAST_ABI_VERSION()

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
