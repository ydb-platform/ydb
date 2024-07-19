UNITTEST_FOR(ydb/library/yql/providers/pq/provider)

SRCS(
    yql_pq_ut.cpp
)

PEERDIR(
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    ydb/library/actors/wilson/protos
    ydb/library/yql/core/facade
    ydb/library/yql/core/file_storage
    ydb/library/yql/core/services/mounts
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/transform
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/yql/providers/dq/local_gateway
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/pq/async_io
    ydb/library/yql/providers/pq/gateway/dummy
    ydb/library/yql/providers/pq/provider
    ydb/library/yql/providers/solomon/gateway
    ydb/library/yql/providers/solomon/provider
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
    ydb/public/sdk/cpp/client/ydb_params
    ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
)

YQL_LAST_ABI_VERSION()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
