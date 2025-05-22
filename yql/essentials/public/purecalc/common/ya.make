LIBRARY()

INCLUDE(ya.make.inc)

PEERDIR(
    yt/yql/providers/yt/codec/codegen
    yql/essentials/providers/config
    yql/essentials/minikql/computation/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/parser/pg_wrapper
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/sql/pg
)

END()

RECURSE(
    no_llvm
)

