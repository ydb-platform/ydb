LIBRARY()

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

INCLUDE(../ya.make.inc)

END()

RECURSE_FOR_TESTS(
    ut
)

