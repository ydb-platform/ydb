LIBRARY()

SRCS(
    optimizer.cpp
    counters.cpp
)
# gn: constructor headers constructor.h

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow
    ydb/core/protos
    ydb/core/tx/columnshard/engines/changes/abstract
)

END()
