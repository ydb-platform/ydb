LIBRARY()

PEERDIR(
    ydb/library/formats/arrow/protos
    ydb/library/formats/arrow/accessor/common
    contrib/libs/apache/arrow
    ydb/library/conclusion
    ydb/library/actors/core
)

SRCS(
    accessor.cpp
)

END()
