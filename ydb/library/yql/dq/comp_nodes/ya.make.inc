INCLUDE(${ARCADIA_ROOT}/yql/essentials/minikql/computation/header.ya.make.inc)

PEERDIR(
    ydb/library/actors/core
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/comp_nodes/hash_join_utils
    ydb/library/yql/dq/runtime
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/computation
    yql/essentials/minikql/invoke_builtins
    yql/essentials/utils
)

SET(ORIG_SRC_DIR ${ARCADIA_ROOT}/ydb/library/yql/dq/comp_nodes)

SET(ORIG_SOURCES
    yql_common_dq_factory.cpp
    dq_hash_aggregate.cpp
    dq_hash_combine.cpp
    dq_hash_operator_common.cpp
    dq_hash_operator_serdes.cpp
    dq_program_builder.cpp
    dq_block_hash_join.cpp
    dq_scalar_hash_join.cpp
    dq_hash_join_table.cpp
    dq_join_common.cpp
    type_utils.cpp
)

YQL_LAST_ABI_VERSION()

ADDINCL(
    ${ORIG_SRC_DIR}
)
CFLAGS(-Wmissing-field-initializers)
COPY(
    WITH_CONTEXT
    AUTO
    FROM ${ORIG_SRC_DIR}
    ${ORIG_SOURCES}
    OUTPUT_INCLUDES
       ${BINDIR}/yql/essentials/minikql/computation/mkql_computation_node_codegen.h
       ${BINDIR}/yql/essentials/minikql/computation/mkql_llvm_base.h
)

PROVIDES(YDB_DQ_COMP_NODES)
