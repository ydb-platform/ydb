LIBRARY()

SRCS(
    read_attributes_utils.cpp
    rewrite_io_utils.cpp
    yql_kikimr_constraints.cpp
    yql_kikimr_datasink.cpp
    yql_kikimr_datasource.cpp
    yql_kikimr_exec.cpp
    yql_kikimr_expr_nodes.h
    yql_kikimr_expr_nodes.cpp
    yql_kikimr_gateway.h
    yql_kikimr_gateway.cpp
    yql_kikimr_opt_build.cpp
    yql_kikimr_opt.cpp
    yql_kikimr_provider.h
    yql_kikimr_provider.cpp
    yql_kikimr_provider_impl.h
    yql_kikimr_results.cpp
    yql_kikimr_results.h
    yql_kikimr_settings.cpp
    yql_kikimr_settings.h
    yql_kikimr_type_ann.cpp
    yql_kikimr_type_ann_pg.h
    yql_kikimr_type_ann_pg.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/docapi
    ydb/core/kqp/expr_nodes
    ydb/core/kqp/opt/cbo
    ydb/core/local_indexes/bloom
    ydb/core/kqp/query_data
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tx/columnshard/engines/storage/indexes/min_max/misc
    ydb/library/aclib
    ydb/library/aclib/protos
    ydb/library/ydb_issue/proto
    ydb/library/yql/dq/common
    ydb/library/yql/dq/constraints
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/opt
    ydb/library/yql/providers/dq/expr_nodes
    ydb/public/lib/scheme_types
    ydb/public/sdk/cpp/src/client/topic
    ydb/services/metadata/optimization
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/core/services
    yql/essentials/core/peephole_opt
    yql/essentials/minikql
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/config
    yql/essentials/providers/common/gateway
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/schema/expr
    yql/essentials/providers/common/transform
    yql/essentials/providers/pg/expr_nodes
    yql/essentials/providers/result/expr_nodes
    yql/essentials/providers/result/provider
    yql/essentials/public/decimal
    yql/essentials/public/issue
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
    yql/essentials/sql
    yql/essentials/sql/settings
    yql/essentials/sql/v1/translation
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

SRCDIR(yql/essentials/core/expr_nodes_gen)

IF(EXPORT_CMAKE)
    RUN_PYTHON3(
        ${ARCADIA_ROOT}/yql/essentials/core/expr_nodes_gen/gen/__main__.py
            yql_expr_nodes_gen.jnj
            yql_kikimr_expr_nodes.json
            yql_kikimr_expr_nodes.gen.h
            yql_kikimr_expr_nodes.decl.inl.h
            yql_kikimr_expr_nodes.defs.inl.h
        IN yql_expr_nodes_gen.jnj
        IN yql_kikimr_expr_nodes.json
        OUT yql_kikimr_expr_nodes.gen.h
        OUT yql_kikimr_expr_nodes.decl.inl.h
        OUT yql_kikimr_expr_nodes.defs.inl.h
        OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h
        ${ARCADIA_ROOT}/util/generic/hash_set.h
    )
ELSE()
    RUN_PROGRAM(
        yql/essentials/core/expr_nodes_gen/gen
            yql_expr_nodes_gen.jnj
            yql_kikimr_expr_nodes.json
            yql_kikimr_expr_nodes.gen.h
            yql_kikimr_expr_nodes.decl.inl.h
            yql_kikimr_expr_nodes.defs.inl.h
        IN yql_expr_nodes_gen.jnj
        IN yql_kikimr_expr_nodes.json
        OUT yql_kikimr_expr_nodes.gen.h
        OUT yql_kikimr_expr_nodes.decl.inl.h
        OUT yql_kikimr_expr_nodes.defs.inl.h
        OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h
        ${ARCADIA_ROOT}/util/generic/hash_set.h
    )
ENDIF()

GENERATE_ENUM_SERIALIZATION(yql_kikimr_provider.h)
GENERATE_ENUM_SERIALIZATION(yql_kikimr_gateway.h)

END()

RECURSE_FOR_TESTS(
    ut
)
