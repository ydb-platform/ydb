UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (NOT SANITIZER_TYPE AND BUILD_TYPE != "DEBUG")
    REQUIREMENTS(cpu:1)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()
SIZE(MEDIUM)

SRCS(
    kqp_fulltext_compact_ut.cpp
    ../fulltext/kqp_fulltext_ut_common.cpp
)

PEERDIR(
    library/cpp/threading/local_executor
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/udfs/common/knn
    yql/essentials/sql/pg_dummy
    ydb/public/sdk/cpp/adapters/issue
)

YQL_LAST_ABI_VERSION()

END()
