LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/switch
    ydb/library/actors/core
    ydb/library/conclusion
)

SRCS(
    container.cpp
    validation.cpp
    adapter.cpp
    accessor.cpp
)

END()
