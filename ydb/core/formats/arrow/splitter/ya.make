LIBRARY()

SRCS(
    stats.cpp
    simple.cpp
    scheme_info.cpp
    similar_packer.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/actors/core
    ydb/library/conclusion
    ydb/core/formats/arrow/serializer
)

END()
