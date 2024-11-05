LIBRARY()

INCLUDE(../ya.make.inc)

PEERDIR(
    ydb/library/yql/providers/yt/codec/codegen/no_llvm
    ydb/library/yql/providers/config
    ydb/library/yql/minikql/computation/no_llvm
    ydb/library/yql/minikql/invoke_builtins/no_llvm
    ydb/library/yql/minikql/comp_nodes/no_llvm
    ydb/library/yql/minikql/codegen/no_llvm
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/library/yql/sql/pg
)

END()

