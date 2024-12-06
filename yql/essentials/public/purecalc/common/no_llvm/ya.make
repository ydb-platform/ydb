LIBRARY()

INCLUDE(../ya.make.inc)

PEERDIR(
    yt/yql/providers/yt/codec/codegen/no_llvm
    yql/essentials/providers/config
    yql/essentials/minikql/computation/no_llvm
    yql/essentials/minikql/invoke_builtins/no_llvm
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/codegen/no_llvm
    yql/essentials/parser/pg_wrapper
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/sql/pg
)

END()

