LIBRARY()

OWNER(
    g:yql
    g:yql_ydb_core
)

CXXFLAGS(-DMKQL_DISABLE_CODEGEN)

ADDINCL(GLOBAL ydb/library/yql/minikql/codegen/llvm_stub)

INCLUDE(../ya.make.inc)

END()
