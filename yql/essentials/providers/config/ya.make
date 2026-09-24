LIBRARY()

SRCS(
    yql_config_provider.cpp
    yql_config_provider.h
    yql_config_flags.cpp
    yql_config_flags.h
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/utils
    yql/essentials/utils/log
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/activation
    yql/essentials/minikql/runtime_settings
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
