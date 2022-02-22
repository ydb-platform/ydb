UNITTEST_FOR(ydb/library/yql/providers/pq/provider)

OWNER(g:yql)

SRCS(
    yql_pq_ut.cpp
)

PEERDIR(
    ydb/core/yq/libs/db_resolver
    ydb/library/yql/core/facade
    ydb/library/yql/core/file_storage
    ydb/library/yql/core/services/mounts
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/minikql/invoke_builtins
    ydb/library/yql/public/udf/service/exception_policy
    ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/dq/local_gateway
    ydb/library/yql/providers/pq/gateway/dummy
    ydb/library/yql/providers/pq/provider
    ydb/library/yql/providers/solomon/gateway
    ydb/library/yql/providers/solomon/provider
)

YQL_LAST_ABI_VERSION()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

END()
