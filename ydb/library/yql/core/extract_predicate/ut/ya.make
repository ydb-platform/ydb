UNITTEST_FOR(ydb/library/yql/core/extract_predicate)

SRCS(
    extract_predicate_ut.cpp
)

PEERDIR(
    library/cpp/yson
    ydb/library/yql/core/facade
    ydb/library/yql/core/services
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/core/type_ann
    ydb/library/yql/core/ut_common
    ydb/library/yql/providers/config
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/result/provider
    ydb/library/yql/providers/yt/gateway/file
    ydb/library/yql/providers/yt/provider
    ydb/library/yql/providers/yt/codec/codegen
    ydb/library/yql/providers/yt/comp_nodes/llvm14
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/minikql/invoke_builtins/llvm14
    ydb/library/yql/sql/pg
)

YQL_LAST_ABI_VERSION()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

END()
