LIBRARY()

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

PEERDIR(
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy

    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/codegen/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16

    library/cpp/testing/unittest

    ydb/core/kqp/runtime

    ydb/library/yql/dq/comp_nodes

    contrib/libs/llvm16/lib/IR
    contrib/libs/llvm16/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm16/lib/Linker
    contrib/libs/llvm16/lib/Passes
    contrib/libs/llvm16/lib/Target/X86
    contrib/libs/llvm16/lib/Target/X86/AsmParser
    contrib/libs/llvm16/lib/Target/X86/Disassembler
    contrib/libs/llvm16/lib/Transforms/IPO
    contrib/libs/llvm16/lib/Transforms/ObjCARC
)

IF (ARCH_X86_64)

CFLAGS(
    -mprfchw
)

ENDIF()

SRCS(
    converters.cpp
    factories.cpp
    printout.cpp
    simple.cpp
    simple_block.cpp
    simple_last.cpp
    subprocess.cpp
    streams.cpp
    tpch_last.cpp
)

END()
