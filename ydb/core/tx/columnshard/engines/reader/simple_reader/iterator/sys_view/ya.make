LIBRARY()

SRCS(
    source.cpp
    constructor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

END()
