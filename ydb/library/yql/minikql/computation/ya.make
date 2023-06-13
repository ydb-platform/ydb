LIBRARY()

SRCS(
    mkql_computation_node.cpp
    mkql_computation_node.h
    mkql_computation_node_codegen.h
    mkql_computation_node_codegen.cpp
    mkql_computation_node_graph.cpp
    mkql_computation_node_graph_saveload.cpp
    mkql_computation_node_graph_saveload.h
    mkql_computation_node_holders.cpp
    mkql_computation_node_impl.h
    mkql_computation_node_impl.cpp
    mkql_computation_node_list.h
    mkql_computation_node_pack.cpp
    mkql_computation_node_pack_impl.cpp
    mkql_computation_node_pack.h
    mkql_computation_node_pack_impl.h
    mkql_custom_list.cpp
    mkql_custom_list.h
    mkql_validate.cpp
    mkql_validate.h
    mkql_value_builder.cpp
    mkql_value_builder.h
    presort.h
    presort.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/enumbitset
    library/cpp/packedtypes
    library/cpp/random_provider
    library/cpp/time_provider
    ydb/library/yql/minikql
    ydb/library/yql/minikql/arrow
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/library/yql/public/udf
    ydb/library/yql/utils
    library/cpp/threading/future
)

IF (NOT MKQL_DISABLE_CODEGEN)
    NO_COMPILER_WARNINGS()
    PEERDIR(
        ydb/library/yql/minikql/codegen
        contrib/libs/llvm12/lib/IR
        contrib/libs/llvm12/lib/ExecutionEngine/MCJIT
        contrib/libs/llvm12/lib/Linker
        contrib/libs/llvm12/lib/Target/X86
        contrib/libs/llvm12/lib/Target/X86/AsmParser
        contrib/libs/llvm12/lib/Transforms/IPO
    )
ELSE()
    CFLAGS(
        -DMKQL_DISABLE_CODEGEN
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
