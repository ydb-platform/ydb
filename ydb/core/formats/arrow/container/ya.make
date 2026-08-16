RECURSE(
    filterable
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/accessor
    ydb/library/actors/core
    ydb/library/conclusion
    ydb/library/formats/arrow
)

SRCS(
    container.cpp
)

END()
