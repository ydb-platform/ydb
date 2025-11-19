LIBRARY()

SRCDIR(yt/yql/providers/yt/codec/codegen/llvm20)

PEERDIR(
    yql/essentials/minikql/codegen/llvm20
)

USE_LLVM_BC20()
SET(LLVM_VER 20)

INCLUDE(../ya.make.inc)

END()
