LIBRARY()

SRCS(
    storage.h
    storage.cpp
)

PEERDIR(
    ydb/library/yql/core/spilling/storage/file_storage
)

NO_COMPILER_WARNINGS()

IF (NOT MKQL_DISABLE_CODEGEN)
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
