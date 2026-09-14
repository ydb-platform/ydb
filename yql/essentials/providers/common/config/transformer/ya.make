LIBRARY()

SRCS(
    yql_configuration_transformer.cpp
)

PEERDIR(
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/providers/common/config
)

YQL_LAST_ABI_VERSION()

END()
