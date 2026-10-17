LIBRARY(library-formats-arrow-scalar)

# gn: move into parent

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/conclusion
    ydb/library/formats/arrow/switch
    ydb/library/actors/core
)

SRCS(
    serialization.cpp
)

END()
