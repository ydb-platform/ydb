LIBRARY()

INCLUDE(ya.make.inc)

PEERDIR(
    ydb/library/yql/providers/yt/codec/codegen
    ydb/library/yql/minikql/computation/llvm
    ydb/library/yql/minikql/invoke_builtins/llvm
    ydb/library/yql/minikql/comp_nodes/llvm
)

END()

RECURSE(
    no_llvm
)

