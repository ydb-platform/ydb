LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/switch
    ydb/library/actors/core
    ydb/library/conclusion
    ydb/core/formats/arrow/splitter
    ydb/core/formats/arrow/validation
)

SRCS(
    container.cpp
    adapter.cpp
)

END()
