
INCLUDE(header.ya.make.inc)

SET(ORIG_SRC_DIR yql/essentials/minikql/computation)

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
    yql/essentials/minikql
    yql/essentials/minikql/arrow
    yql/essentials/minikql/computation
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/utils
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
        ${BINDIR}/yql/essentials/minikql/computation/mkql_llvm_base.h
        ${BINDIR}/yql/essentials/minikql/computation/mkql_computation_node_codegen.h
)

PROVIDES(MINIKQL_COMPUTATION)
