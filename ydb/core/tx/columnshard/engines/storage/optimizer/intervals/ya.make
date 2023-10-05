LIBRARY()

SRCS(
    optimizer.cpp
    blob_size.cpp
    counters.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/changes/abstract
)

END()
