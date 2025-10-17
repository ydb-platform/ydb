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
    yql/essentials/core
    yql/essentials/core/type_ann
    yql/essentials/core/dq_integration
    yql/essentials/providers/common/dq
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/transform
    yql/essentials/providers/pg/expr_nodes
    yql/essentials/parser/pg_catalog
    yql/essentials/utils/log
)

END()
