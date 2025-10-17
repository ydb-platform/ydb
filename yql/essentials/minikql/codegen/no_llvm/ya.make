LIBRARY()

ENABLE(YQL_STYLE_CPP)

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
