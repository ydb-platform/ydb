LIBRARY()

PEERDIR(
    yql/essentials/minikql/codegen/llvm20
    contrib/libs/llvm20/lib/IR
    contrib/libs/llvm20/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm20/lib/Linker
    contrib/libs/llvm20/lib/Passes
    contrib/libs/llvm20/lib/Transforms/IPO
    contrib/libs/llvm20/lib/Transforms/ObjCARC
)

IF (ARCH_X86_64)
    PEERDIR(
        contrib/libs/llvm20/lib/Target/X86
        contrib/libs/llvm20/lib/Target/X86/AsmParser
        contrib/libs/llvm20/lib/Target/X86/Disassembler
    )
ELSEIF (ARCH_AARCH64)
    PEERDIR(
        contrib/libs/llvm20/lib/Target/AArch64
        contrib/libs/llvm20/lib/Target/AArch64/AsmParser
        contrib/libs/llvm20/lib/Target/AArch64/Disassembler
    )
ENDIF()

INCLUDE(../ya.make.inc)

END()

RECURSE_FOR_TESTS(
    ut
)
