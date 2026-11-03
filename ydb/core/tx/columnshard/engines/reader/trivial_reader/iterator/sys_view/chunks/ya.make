LIBRARY()

SRCS(
    source.cpp
    constructor.cpp
    metadata.cpp
    GLOBAL schema.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    ydb/core/tx/tiering/tier
)

END()
