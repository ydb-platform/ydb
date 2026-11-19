LIBRARY()

SRCS(
    read_metadata.cpp
    resolver.cpp
)

PEERDIR(
    ydb/core/kqp/common/events
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/core/tx/columnshard/engines/reader/common
)

END()
