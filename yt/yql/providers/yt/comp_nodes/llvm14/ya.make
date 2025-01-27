LIBRARY()

NO_COMPILER_WARNINGS()

PEERDIR(
    yql/essentials/minikql/codegen/llvm14
    yql/essentials/minikql/invoke_builtins/llvm14
    yt/yql/providers/yt/codec/codegen/llvm14
)

INCLUDE(../ya.make.inc)

END()
