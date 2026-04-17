LIBRARY()

SRCS(
    abstract.cpp
    result.cpp
    limit.cpp
    aggr.cpp
    distinct_limit.cpp
)

PEERDIR(
    ydb/core/formats/arrow
)

END()
