LIBRARY()

SRCS(
    optimizer.cpp
    counters.cpp
)

PEERDIR(
    contrib/libs/apache/arrow_next
    ydb/core/formats/arrow
    ydb/core/protos
    ydb/core/tx/columnshard/engines/changes/abstract
)

END()
