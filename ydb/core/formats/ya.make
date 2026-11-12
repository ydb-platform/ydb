RECURSE(
    arrow
)

LIBRARY()

PEERDIR(
    ydb/core/scheme
)

SRCS(
    clickhouse_block.h
    clickhouse_block.cpp
    factory.h
)

END()
