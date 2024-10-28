LIBRARY()

INCLUDE(ya.make.inc)

PEERDIR(
    ydb/library/yql/providers/yt/codec/codegen
    ydb/library/yql/providers/config
    ydb/library/yql/minikql/computation/llvm14
    ydb/library/yql/minikql/invoke_builtins/llvm14
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/library/yql/sql/pg
)

END()

RECURSE(
    no_llvm
)

