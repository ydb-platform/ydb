LIBRARY()

SRCS(
    simple.cpp
    scheme_info.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/actors/core
    ydb/library/conclusion
    ydb/library/formats/arrow/splitter
    ydb/library/formats/arrow/common
    ydb/core/formats/arrow/serializer
)

END()
