LIBRARY()

SRCS(
    view.cpp
    view_v0.cpp
    collection.cpp
)

PEERDIR(
    ydb/library/conclusion
    contrib/libs/apache/arrow_next
    ydb/library/actors/core
    ydb/core/formats/arrow/reader
)

END()
