UNITTEST_FOR(ydb/library/yql/providers/dq/provider)

SRCS(
    yql_dq_provider_ut.cpp
)

PEERDIR(
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/transform
    ydb/library/yql/providers/dq/local_gateway
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/dq/provider/exec
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    library/cpp/testing/unittest
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm16
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/lib/ut_common
    yt/yql/providers/yt/provider
    yql/essentials/core/cbo/simple
    yql/essentials/core/facade
    yql/essentials/core/file_storage
    yql/essentials/core/services/mounts
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/providers/common/comp_nodes
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
