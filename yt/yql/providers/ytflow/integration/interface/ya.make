LIBRARY()

YQL_LAST_ABI_VERSION()

PEERDIR(
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/minikql
    yt/yql/providers/yt/lib/yt_token_resolver
)

SRCS(
    yql_ytflow_integration.cpp
    yql_ytflow_optimization.cpp
)

END()
