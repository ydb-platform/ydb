LIBRARY()

SRCS(
    stats.cpp
    similar_packer.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/actors/core
    ydb/library/conclusion
)

END()
