LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/formats/arrow/common
)

SRCS(
    accessor.cpp
)

END()
