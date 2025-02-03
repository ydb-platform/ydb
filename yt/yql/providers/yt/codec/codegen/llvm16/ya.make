LIBRARY()

SRCDIR(yt/yql/providers/yt/codec/codegen/llvm16)

PEERDIR(
    yql/essentials/minikql/codegen/llvm16
)

USE_LLVM_BC16()
SET(LLVM_VER 16)

INCLUDE(../ya.make.inc)

END()
