LIBRARY()

SRCS(
    source.cpp
    constructor.cpp
    metadata.cpp
    schema.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

END()
