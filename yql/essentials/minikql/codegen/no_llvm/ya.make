LIBRARY()

SRCDIR(
    yql/essentials/minikql/codegen
)

ADDINCL(
    yql/essentials/minikql/codegen
)

SRCS(
    codegen_dummy.cpp
)

PROVIDES(MINIKQL_CODEGEN)

END()
