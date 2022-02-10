LIBRARY()

OWNER(spuchin)

SRCS(
    kqp_opt_helpers.cpp
    yql_kikimr_datasink.cpp
    yql_kikimr_datasource.cpp
    yql_kikimr_exec.cpp
    yql_kikimr_expr_nodes.h
    yql_kikimr_expr_nodes.cpp
    yql_kikimr_gateway.h
    yql_kikimr_gateway.cpp
    yql_kikimr_kql.cpp
    yql_kikimr_mkql.cpp
    yql_kikimr_opt_build.cpp
    yql_kikimr_opt_join.cpp
    yql_kikimr_opt_range.cpp
    yql_kikimr_opt_utils.cpp
    yql_kikimr_opt.cpp
    yql_kikimr_provider.h
    yql_kikimr_provider.cpp
    yql_kikimr_provider_impl.h
    yql_kikimr_query_traits.cpp
    yql_kikimr_results.cpp
    yql_kikimr_results.h
    yql_kikimr_settings.cpp
    yql_kikimr_settings.h
    yql_kikimr_type_ann.cpp
)

PEERDIR(
    ydb/core/base 
    ydb/core/kqp/provider/mkql 
    ydb/core/protos 
    ydb/library/aclib 
    ydb/library/aclib/protos 
    ydb/library/binary_json 
    ydb/library/dynumber 
    ydb/library/yql/core/services 
    ydb/library/yql/minikql 
    ydb/library/yql/public/decimal 
    ydb/public/lib/scheme_types 
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/config
    ydb/library/yql/providers/common/gateway
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/schema/expr
    ydb/library/yql/providers/result/expr_nodes
    ydb/library/yql/providers/result/provider
)

YQL_LAST_ABI_VERSION() 

SRCDIR(ydb/library/yql/core/expr_nodes_gen) 

RUN_PROGRAM(
    ydb/library/yql/core/expr_nodes_gen/gen yql_expr_nodes_gen.jnj yql_kikimr_expr_nodes.json 
        yql_kikimr_expr_nodes.gen.h yql_kikimr_expr_nodes.decl.inl.h yql_kikimr_expr_nodes.defs.inl.h 
    IN yql_expr_nodes_gen.jnj
    IN yql_kikimr_expr_nodes.json
    OUT yql_kikimr_expr_nodes.gen.h
    OUT yql_kikimr_expr_nodes.decl.inl.h
    OUT yql_kikimr_expr_nodes.defs.inl.h
    OUTPUT_INCLUDES ${ARCADIA_ROOT}/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h 
        ${ARCADIA_ROOT}/util/generic/hash_set.h 
)

GENERATE_ENUM_SERIALIZATION(yql_kikimr_provider.h)

END()

RECURSE_FOR_TESTS( 
    ut 
) 
