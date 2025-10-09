LIBRARY()

NO_COMPILER_WARNINGS()

PEERDIR(
    yql/essentials/minikql/codegen/llvm20
    yql/essentials/minikql/invoke_builtins/llvm20
    yt/yql/providers/yt/codec/codegen/llvm20
)

INCLUDE(../ya.make.inc)

END()
