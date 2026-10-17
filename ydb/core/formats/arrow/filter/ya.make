YQL_LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/switch
    ydb/library/accessor
    ydb/library/actors/core
    ydb/library/conclusion
    ydb/library/formats/arrow
    ydb/library/yverify_stream
)

SRCS(
    filter.cpp  # gn: into ydb/core/formats/arrow
    filter.h  # gn: into ydb/core/formats/arrow
)

END()
