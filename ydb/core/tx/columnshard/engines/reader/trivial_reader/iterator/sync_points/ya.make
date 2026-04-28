LIBRARY()

SRCS(
    abstract.cpp
    result.cpp
    limit.cpp
    aggr.cpp
)

PEERDIR(
    ydb/core/formats/arrow
)

END()
