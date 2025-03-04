LIBRARY()

SRCS(
    create_table_formatter.cpp
    create_table_formatter.h
    show_create.cpp
    show_create.h
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/protos
    ydb/core/sys_view/common
    ydb/core/tx/tx_proxy
    ydb/core/ydb_convert
    ydb/library/actors/core
    ydb/public/api/protos
    ydb/public/lib/json_value
    ydb/public/lib/ydb_cli/dump/util
    ydb/public/sdk/cpp/src/client/types
    yql/essentials/ast
)

YQL_LAST_ABI_VERSION()

END()
