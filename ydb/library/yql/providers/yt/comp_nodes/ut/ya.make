UNITTEST_FOR(ydb/library/yql/providers/yt/comp_nodes)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    yql_mkql_output_ut.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/time_provider
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/providers/yt/comp_nodes/llvm14
    ydb/library/yql/providers/yt/codec
    ydb/library/yql/providers/yt/codec/codegen
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
