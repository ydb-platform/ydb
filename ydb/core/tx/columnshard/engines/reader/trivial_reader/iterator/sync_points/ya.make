LIBRARY()

SRCS(
    abstract.cpp
    result.cpp
    limit.cpp
    aggr.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/reader/abstract
)

END()
