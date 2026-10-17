LIBRARY()

SRCS(
    manager.cpp
    collector.cpp
    constructor.cpp  # gn: constructor
)
# gn: constructor headers constructor.h

PEERDIR(
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/engines/reader/tracing
    ydb/core/protos
)

END()
