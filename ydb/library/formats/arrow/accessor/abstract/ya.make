LIBRARY()

PEERDIR(
    ydb/library/formats/arrow/protos
    ydb/library/formats/arrow/accessor/common
    contrib/libs/apache/arrow
    ydb/library/conclusion
)

SRCS(
    accessor.cpp
)

END()
