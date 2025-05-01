LIBRARY()

SRCS(
    view.cpp
    collection.cpp
)

PEERDIR(
    ydb/library/conclusion
    contrib/libs/apache/arrow
    ydb/library/actors/core
)

END()
