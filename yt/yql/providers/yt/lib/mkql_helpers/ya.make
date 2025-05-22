LIBRARY()

SRCS(
    mkql_helpers.cpp
)

PEERDIR(
    yql/essentials/minikql
    yql/essentials/core
    yql/essentials/ast
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()
