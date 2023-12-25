UNITTEST()

PEERDIR(
    contrib/libs/llvm12/lib/IR
    contrib/libs/llvm12/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm12/lib/Linker
    contrib/libs/llvm12/lib/Target/X86
    contrib/libs/llvm12/lib/Target/X86/AsmParser
    contrib/libs/llvm12/lib/Target/X86/Disassembler
    contrib/libs/llvm12/lib/Transforms/IPO
    contrib/libs/llvm12/lib/Transforms/ObjCARC
)

IF (OS_LINUX)
    PEERDIR(
        contrib/libs/llvm12/lib/ExecutionEngine/PerfJITEvents
    )
ENDIF()

SET(LLVM_VER 12)

INCLUDE(../../ut/ya.make.inc)

PEERDIR(ydb/library/yql/minikql/codegen/llvm)

END()

