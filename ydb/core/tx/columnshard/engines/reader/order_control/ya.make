LIBRARY()

SRCS(
    abstract.cpp
    not_sorted.cpp
    pk_with_limit.cpp
    default.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
)

END()
