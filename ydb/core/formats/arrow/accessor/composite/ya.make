LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/common
)

SRCS(
    accessor.cpp
)

END()
