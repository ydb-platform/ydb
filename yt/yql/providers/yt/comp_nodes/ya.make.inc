INCLUDE(${ARCADIA_ROOT}/yql/essentials/minikql/computation/header.ya.make.inc)
INCLUDE(${ARCADIA_ROOT}/yql/essentials/minikql/invoke_builtins/header.ya.make.inc)

SET(ORIG_SRC_DIR ${ARCADIA_ROOT}/yt/yql/providers/yt/comp_nodes)

SET(ORIG_SOURCES
    yql_mkql_file_block_stream.cpp
    yql_mkql_file_input_state.cpp
    yql_mkql_file_list.cpp
    yql_mkql_input_stream.cpp
    yql_mkql_input.cpp
    yql_mkql_output.cpp
    yql_mkql_block_table_content.cpp
    yql_mkql_table_content.cpp
    yql_mkql_table.cpp
    yql_mkql_ungrouping_list.cpp
)

PEERDIR(
    library/cpp/streams/brotli
    yql/essentials/minikql
    yql/essentials/public/udf
    yql/essentials/utils
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/mkql
    yt/yql/providers/yt/codec
    yt/yql/providers/yt/expr_nodes
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
        ${BINDIR}/yql/essentials/minikql/computation/mkql_computation_node_codegen.h
        ${BINDIR}/yql/essentials/minikql/invoke_builtins/mkql_builtins_codegen.h
        ${BINDIR}/yql/essentials/minikql/invoke_builtins/mkql_builtins_decimal.h
)

PROVIDES(YT_COMP_NODES)
