UNITTEST_FOR(ydb/library/yql/providers/pq/provider)

SRCS(
    yql_pq_ut.cpp
)

PEERDIR(
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    ydb/library/actors/wilson/protos
    yql/essentials/core/facade
    yql/essentials/core/file_storage
    yql/essentials/core/services/mounts
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/transform
    yql/essentials/minikql/comp_nodes/llvm14
    yql/essentials/providers/common/comp_nodes
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/yql/providers/dq/local_gateway
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/pq/async_io
    ydb/library/yql/providers/pq/gateway/dummy
    ydb/library/yql/providers/pq/provider
    ydb/library/yql/providers/solomon/gateway
    ydb/library/yql/providers/solomon/provider
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    ydb/public/sdk/cpp/src/client/params
    ydb/public/sdk/cpp/src/client/persqueue_public/codecs
)

YQL_LAST_ABI_VERSION()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
