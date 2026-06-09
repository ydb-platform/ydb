LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow
    ydb/core/formats/arrow/accessor/plain
    ydb/core/formats/arrow/splitter
    ydb/core/formats/arrow/switch
    ydb/library/actors/core
    ydb/library/conclusion
    ydb/library/formats/arrow
)

SRCS(
    container.cpp
    adapter.cpp
)

END()
