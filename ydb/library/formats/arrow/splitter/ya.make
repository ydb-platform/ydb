LIBRARY(library-formats-arrow-splitter)

SRCS(
    stats.cpp
    similar_packer.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/actors/core
    ydb/library/conclusion
)

GENERATE_ENUM_SERIALIZATION(stats.h)

END()
