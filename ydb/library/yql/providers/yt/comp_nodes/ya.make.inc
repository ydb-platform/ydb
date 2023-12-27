INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/minikql/computation/header.ya.make.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/minikql/invoke_builtins/header.ya.make.inc)

SET(ORIG_SRC_DIR ${ARCADIA_ROOT}/ydb/library/yql/providers/yt/comp_nodes)

SET(ORIG_SOURCES
    yql_mkql_file_input_state.cpp
    yql_mkql_file_list.cpp
    yql_mkql_input_stream.cpp
    yql_mkql_input.cpp
    yql_mkql_output.cpp
    yql_mkql_table_content.cpp
    yql_mkql_table.cpp
    yql_mkql_ungrouping_list.cpp
)

PEERDIR(
    library/cpp/streams/brotli
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
    ydb/library/yql/utils
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/yt/codec
    ydb/library/yql/providers/yt/expr_nodes
)

YQL_LAST_ABI_VERSION()

ADDINCL(
    ${ORIG_SRC_DIR}
)

COPY(
    WITH_CONTEXT
    AUTO
    FROM ${ORIG_SRC_DIR}
    ${ORIG_SOURCES}
    OUTPUT_INCLUDES 
    ${BINDIR}/ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h
    ${BINDIR}/ydb/library/yql/minikql/invoke_builtins/mkql_builtins_codegen.h
    ${BINDIR}/ydb/library/yql/minikql/invoke_builtins/mkql_builtins_decimal.h
)

PROVIDES(YT_COMP_NODES)
