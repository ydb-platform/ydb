UNITTEST()

PEERDIR(
    contrib/libs/llvm20/lib/IR
    contrib/libs/llvm20/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm20/lib/Linker
    contrib/libs/llvm20/lib/Passes
    contrib/libs/llvm20/lib/Target/X86
    contrib/libs/llvm20/lib/Target/X86/AsmParser
    contrib/libs/llvm20/lib/Target/X86/Disassembler
    contrib/libs/llvm20/lib/Transforms/IPO
    contrib/libs/llvm20/lib/Transforms/ObjCARC
)

IF (OS_LINUX)
    PEERDIR(
        contrib/libs/llvm20/lib/ExecutionEngine/PerfJITEvents
    )
ENDIF()

USE_LLVM_BC20()
SET(LLVM_VER 20)

INCLUDE(../../ut/ya.make.inc)

PEERDIR(yql/essentials/minikql/codegen/llvm20)

END()
