UNITTEST()

TAG(ya:manual)

PEERDIR(
    contrib/libs/llvm14/lib/IR
    contrib/libs/llvm14/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm14/lib/Linker
    contrib/libs/llvm14/lib/Target/X86
    contrib/libs/llvm14/lib/Target/X86/AsmParser
    contrib/libs/llvm14/lib/Target/X86/Disassembler
    contrib/libs/llvm14/lib/Transforms/IPO
    contrib/libs/llvm14/lib/Transforms/ObjCARC
)

IF (OS_LINUX)
    PEERDIR(
        contrib/libs/llvm14/lib/ExecutionEngine/PerfJITEvents
    )
ENDIF()

USE_LLVM_BC14()
SET(LLVM_VER 14)

INCLUDE(../../ut/ya.make.inc)

PEERDIR(ydb/library/yql/minikql/codegen/llvm14)

END()
