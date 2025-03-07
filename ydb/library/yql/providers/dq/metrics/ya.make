LIBRARY()

SET(
    SOURCE
    metrics_printer.cpp
)

SRCS(
    ${SOURCE}
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/yql/providers/solomon/actors
)

YQL_LAST_ABI_VERSION()

END()
