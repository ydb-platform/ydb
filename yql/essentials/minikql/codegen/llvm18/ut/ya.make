UNITTEST()

PEERDIR(
    contrib/libs/llvm18/lib/IR
    contrib/libs/llvm18/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm18/lib/Linker
    contrib/libs/llvm18/lib/Passes
    contrib/libs/llvm18/lib/Target/X86
    contrib/libs/llvm18/lib/Target/X86/AsmParser
    contrib/libs/llvm18/lib/Target/X86/Disassembler
    contrib/libs/llvm18/lib/Transforms/IPO
    contrib/libs/llvm18/lib/Transforms/ObjCARC
)

IF (OS_LINUX)
    PEERDIR(
        contrib/libs/llvm18/lib/ExecutionEngine/PerfJITEvents
    )
ENDIF()

USE_LLVM_BC18()
SET(LLVM_VER 18)

INCLUDE(../../ut/ya.make.inc)

PEERDIR(yql/essentials/minikql/codegen/llvm18)

END()

