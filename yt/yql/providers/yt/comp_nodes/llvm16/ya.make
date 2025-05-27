LIBRARY()

NO_COMPILER_WARNINGS()

PEERDIR(
    yql/essentials/minikql/codegen/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yt/yql/providers/yt/codec/codegen/llvm16
)

INCLUDE(../ya.make.inc)

END()
