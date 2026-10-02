LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/accessor/plain
    ydb/core/formats/arrow/container
    ydb/core/formats/arrow/splitter
    ydb/core/formats/arrow/switch
    ydb/library/actors/core
    ydb/library/conclusion
    ydb/library/formats/arrow
)

SRCS(
    adapter.cpp
)

END()
