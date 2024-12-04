LIBRARY()

SRCS(
    yql_config_provider.cpp
    yql_config_provider.h
)

PEERDIR(
    library/cpp/json
    yql/essentials/ast
    yql/essentials/utils
    yql/essentials/utils/fetch
    yql/essentials/utils/log
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/activation
)

YQL_LAST_ABI_VERSION()

END()
