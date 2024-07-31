OWNER(
    g:yql
    g:yql_ydb_core
)


INCLUDE(header.ya.make.inc)

SET(ORIG_SRC_DIR ydb/library/yql/minikql/computation)

SET(ORIG_SOURCES
    mkql_computation_node_codegen.cpp
    mkql_simple_codegen.cpp
    mkql_computation_node_graph.cpp
    mkql_computation_node_graph_saveload.cpp
    mkql_computation_node_holders_codegen.cpp
    mkql_llvm_base.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/enumbitset
    library/cpp/packedtypes
    library/cpp/random_provider
    library/cpp/time_provider
    ydb/library/yql/minikql
    ydb/library/yql/minikql/arrow
    ydb/library/yql/minikql/computation
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/library/yql/public/udf
    ydb/library/yql/utils
    library/cpp/threading/future
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
        ${BINDIR}/ydb/library/yql/minikql/computation/mkql_llvm_base.h
        ${BINDIR}/ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h
)

PROVIDES(MINIKQL_COMPUTATION)
