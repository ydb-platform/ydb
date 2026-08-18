LIBRARY()

SRCS(
    show_create.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/protos
    ydb/core/sys_view/common
    ydb/core/sys_view/show_create/formatters
    ydb/core/tx/schemeshard
    ydb/core/tx/sequenceproxy
    ydb/core/tx/tx_proxy
    ydb/library/actors/core
)

YQL_LAST_ABI_VERSION()

END()
