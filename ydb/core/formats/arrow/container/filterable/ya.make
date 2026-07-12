LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/container
    ydb/core/formats/arrow/filter
)

SRCS(
    filterable.cpp
)

END()
