LIBRARY()

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

INCLUDE(../ya.make.inc)

END()

RECURSE_FOR_TESTS(
    ut
)
