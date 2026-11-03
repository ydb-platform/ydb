YQL_LIBRARY()

SRCS(
    saver.cpp
    loader.cpp
)

PEERDIR(
    ydb/core/formats/arrow/reader/stats
    ydb/library/actors/core
    contrib/libs/apache/arrow
    ydb/library/accessor
    ydb/library/conclusion
    ydb/library/formats/arrow/transformer
    ydb/library/formats/arrow/common
    ydb/core/formats/arrow/transformer
    ydb/core/formats/arrow/serializer
)

END()
