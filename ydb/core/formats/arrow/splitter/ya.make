YQL_LIBRARY()

SRCS(
    simple.cpp  # gn: into ydb/core/formats/arrow
    scheme_info.cpp  # gn: into ydb/core/formats/arrow
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
