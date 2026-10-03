LIBRARY()

SRCS(
    optimizer.cpp
    counters.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/optimizer/abstract/interface
    contrib/libs/apache/arrow
    ydb/core/formats/arrow
    ydb/core/protos
    ydb/core/tx/columnshard/engines/changes/abstract
)

END()
