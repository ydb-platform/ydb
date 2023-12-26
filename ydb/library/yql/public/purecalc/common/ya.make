LIBRARY()

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
    transformations/type_annotation.cpp
    type_from_schema.cpp
    worker.cpp
    worker_factory.cpp
    wrappers.cpp
)

PEERDIR(
    ydb/library/yql/sql/pg
    ydb/library/yql/ast
    ydb/library/yql/core/services
    ydb/library/yql/core/services/mounts
    ydb/library/yql/core/user_data
    ydb/library/yql/minikql/computation/llvm
    ydb/library/yql/minikql/invoke_builtins/llvm
    ydb/library/yql/minikql/comp_nodes/llvm
    ydb/library/yql/utils/backtrace
    ydb/library/yql/utils/log
    ydb/library/yql/core
    ydb/library/yql/core/type_ann
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/schema/expr
    ydb/library/yql/providers/common/udf_resolve
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(interface.h)

END()
