UNITTEST()

PEERDIR(
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

IF (OS_LINUX)
    PEERDIR(
        contrib/libs/llvm16/lib/ExecutionEngine/PerfJITEvents
    )
ENDIF()

USE_LLVM_BC16()
SET(LLVM_VER 16)

INCLUDE(../../ut/ya.make.inc)

PEERDIR(yql/essentials/minikql/codegen/llvm16)

END()
