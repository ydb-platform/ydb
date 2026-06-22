LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/container
    ydb/core/formats/arrow/switch
    ydb/library/actors/core
    ydb/library/conclusion
    ydb/library/formats/arrow
    ydb/core/formats/arrow/splitter
)

SRCS(
    adapter.cpp
)

END()
