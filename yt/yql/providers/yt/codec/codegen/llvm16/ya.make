LIBRARY()

SRCDIR(yt/yql/providers/yt/codec/codegen/llvm16)

PEERDIR(
    yql/essentials/minikql/codegen/llvm16
)

USE_LLVM_BC16()
SET(LLVM_VER 16)

# mcd/dc coverage is introduced in Clang18
NO_CLANG_MCDC_COVERAGE()

INCLUDE(../ya.make.inc)

END()
