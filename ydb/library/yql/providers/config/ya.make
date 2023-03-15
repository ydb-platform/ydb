LIBRARY()

SRCS(
    yql_config_provider.cpp
    yql_config_provider.h
)

PEERDIR(
    library/cpp/json
    ydb/library/yql/ast
    ydb/library/yql/utils
    ydb/library/yql/utils/fetch
    ydb/library/yql/utils/log
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/activation
)

YQL_LAST_ABI_VERSION()

END()
