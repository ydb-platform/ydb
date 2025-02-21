SRCDIR(
    yql/essentials/public/purecalc/common
)

ADDINCL(
    yql/essentials/public/purecalc/common
)

SRCS(
    compile_mkql.cpp
    fwd.cpp
    inspect_input.cpp
    interface.cpp
    logger_init.cpp
    names.cpp
    processor_mode.cpp
    program_factory.cpp
    transformations/align_output_schema.cpp
    transformations/extract_used_columns.cpp
    transformations/output_columns_filter.cpp
    transformations/replace_table_reads.cpp
    transformations/root_to_blocks.cpp
    transformations/type_annotation.cpp
    transformations/utils.cpp
    type_from_schema.cpp
    worker.cpp
    worker_factory.cpp
    wrappers.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core/services
    yql/essentials/core/services/mounts
    yql/essentials/core/user_data
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
    yql/essentials/core
    yql/essentials/core/type_ann
    yql/essentials/sql
    yql/essentials/sql/v1
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    #{ FIXME
    yql/essentials/sql/v1/lexer/antlr3
    yql/essentials/sql/v1/lexer/antlr3_ansi
    yql/essentials/sql/v1/proto_parser/antlr3
    yql/essentials/sql/v1/proto_parser/antlr3_ansi
    #}
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/comp_nodes
    yql/essentials/providers/common/mkql
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/schema/expr
    yql/essentials/providers/common/udf_resolve
    yql/essentials/providers/common/arrow_resolve
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(interface.h)

