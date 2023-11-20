LIBRARY()

SET(
    SOURCE
    metrics_printer.cpp
)

SRCS(
    ${SOURCE}
)

PEERDIR(
    library/cpp/actors/core
    ydb/library/yql/providers/solomon/async_io
)

YQL_LAST_ABI_VERSION()

END()
