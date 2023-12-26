LIBRARY()

SRCDIR(
    ydb/library/yql/minikql/codegen
)

ADDINCL(
    ydb/library/yql/minikql/codegen
)

SRCS(
    codegen_dummy.cpp
)

PROVIDES(MINIKQL_CODEGEN)

END()
