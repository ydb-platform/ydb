LIBRARY()

PEERDIR(
    yql/essentials/minikql/codegen/llvm16
    contrib/libs/llvm16/lib/IR
    contrib/libs/llvm16/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm16/lib/Linker
    contrib/libs/llvm16/lib/Passes
    contrib/libs/llvm16/lib/Transforms/IPO
    contrib/libs/llvm16/lib/Transforms/ObjCARC
)

IF (ARCH_X86_64)
    PEERDIR(
        contrib/libs/llvm16/lib/Target/X86
        contrib/libs/llvm16/lib/Target/X86/AsmParser
        contrib/libs/llvm16/lib/Target/X86/Disassembler
    )
ELSEIF (ARCH_AARCH64)
    PEERDIR(
        contrib/libs/llvm16/lib/Target/AArch64
        contrib/libs/llvm16/lib/Target/AArch64/AsmParser
        contrib/libs/llvm16/lib/Target/AArch64/Disassembler
    )
ENDIF()

INCLUDE(../ya.make.inc)

END()

RECURSE_FOR_TESTS(
    ut
)
