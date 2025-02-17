LIBRARY()

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

INCLUDE(../ya.make.inc)

END()

RECURSE_FOR_TESTS(
    ut
)
