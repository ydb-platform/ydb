LIBRARY()

SRCS(
    yql_pg_datasink.cpp
    yql_pg_datasink_execution.cpp
    yql_pg_datasink_type_ann.cpp
    yql_pg_datasource.cpp
    yql_pg_dq_integration.cpp
    yql_pg_datasource_type_ann.cpp
    yql_pg_provider.cpp
    yql_pg_provider.h
    yql_pg_provider_impl.h
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    ydb/library/yql/core
    ydb/library/yql/core/type_ann
    ydb/library/yql/dq/integration
    ydb/library/yql/providers/common/dq
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/transform
    ydb/library/yql/providers/pg/expr_nodes
    ydb/library/yql/parser/pg_catalog
    ydb/library/yql/utils/log
)

END()
