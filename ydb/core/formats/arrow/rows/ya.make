YQL_LIBRARY()

SRCS(
    view.cpp  # gn: into ydb/core/formats/arrow
    view_v0.cpp  # gn: into ydb/core/formats/arrow
    collection.cpp  # gn: into ydb/core/formats/arrow
)

PEERDIR(
    ydb/library/conclusion
    contrib/libs/apache/arrow
    ydb/library/actors/core
    ydb/core/formats/arrow/reader
)

END()
