LIBRARY()

SRCS(
    create_table_formatter.cpp
    create_view_formatter.cpp
    formatters_common.cpp
    show_create.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/protos
    ydb/core/sys_view/common
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy
    ydb/core/ydb_convert
    ydb/library/actors/core
    ydb/public/api/protos
    ydb/public/lib/json_value
    ydb/public/lib/ydb_cli/dump/util
    ydb/public/sdk/cpp/src/client/types
    yql/essentials/ast
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
