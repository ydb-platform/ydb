UNITTEST_FOR(ydb/core/kqp/provider)

SRCS(
    read_attributes_utils_ut.cpp
    sql_path_aliases_ut.cpp
    yql_kikimr_gateway_ut.cpp
    yql_kikimr_provider_ut.cpp
    yql_kikimr_provider_streaming_ut.cpp
)

PEERDIR(
    library/cpp/testing/gmock_in_unittest
    ydb/core/kqp/opt
    ydb/core/kqp/query_compiler
    ydb/core/kqp/ut/common
    ydb/core/resource_pools
    yql/essentials/ast
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
    yql/essentials/sql/v1/translation
)

YQL_LAST_ABI_VERSION()

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ENDIF()

END()
