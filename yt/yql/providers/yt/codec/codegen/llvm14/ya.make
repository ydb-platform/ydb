LIBRARY()

SRCDIR(yt/yql/providers/yt/codec/codegen/llvm14)

PEERDIR(
    yql/essentials/minikql/codegen/llvm14
)

USE_LLVM_BC14()
SET(LLVM_VER 14)

INCLUDE(../ya.make.inc)

END()
