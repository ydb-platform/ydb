LIBRARY()

SRCS(
    read_attributes_utils.cpp
    rewrite_io_utils.cpp
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
    ydb/core/protos
    ydb/core/docapi
    ydb/core/scheme
    ydb/core/kqp/query_data
    ydb/library/aclib
    ydb/library/aclib/protos
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
    yql/essentials/core/services
    yql/essentials/minikql
    yql/essentials/public/decimal
    ydb/public/lib/scheme_types
    ydb/public/sdk/cpp/src/client/topic
    yql/essentials/core/expr_nodes
    yql/essentials/core/peephole_opt
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/config
    yql/essentials/providers/common/gateway
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/schema/expr
    ydb/library/yql/providers/dq/expr_nodes
    yql/essentials/providers/pg/expr_nodes
    yql/essentials/providers/result/expr_nodes
    yql/essentials/providers/result/provider
    yql/essentials/sql
    yql/essentials/sql/v1
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/sql/v1/lexer/antlr3
    yql/essentials/sql/v1/lexer/antlr3_ansi
    yql/essentials/sql/v1/proto_parser/antlr3
    yql/essentials/sql/v1/proto_parser/antlr3_ansi
    ydb/library/ydb_issue/proto
    yql/essentials/public/issue
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

SRCDIR(yql/essentials/core/expr_nodes_gen)

RUN_PY3_PROGRAM(
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

GENERATE_ENUM_SERIALIZATION(yql_kikimr_provider.h)

END()

RECURSE_FOR_TESTS(
    ut
)
