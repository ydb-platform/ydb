LIBRARY()

INCLUDE(ya.make.inc)

PEERDIR(
    yt/yql/providers/yt/codec/codegen
    yql/essentials/providers/config
    yql/essentials/minikql/computation/llvm14
    yql/essentials/minikql/invoke_builtins/llvm14
    yql/essentials/minikql/comp_nodes/llvm14
    yql/essentials/parser/pg_wrapper
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/sql/pg
)

END()

RECURSE(
    no_llvm
)

