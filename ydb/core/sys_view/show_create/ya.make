YQL_LIBRARY()

SRCS(
    create_external_table_formatter.cpp
    show_create.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/kqp/runtime/common
    ydb/core/protos
    ydb/core/sys_view/common
    ydb/core/sys_view/show_create/formatters
    ydb/core/tx/columnshard/engines/scheme/defaults/protos
    ydb/core/tx/schemeshard
    ydb/core/tx/sequenceproxy
    ydb/core/tx/tx_proxy
    ydb/core/ydb_convert
    ydb/library/actors/core
    ydb/public/api/protos
    ydb/public/lib/ydb_cli/dump/util
)

END()

RECURSE(
    formatters
)
