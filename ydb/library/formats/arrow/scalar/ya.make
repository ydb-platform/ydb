LIBRARY(library-formats-arrow-scalar)

PEERDIR(
    contrib/libs/apache/arrow_next
    ydb/library/conclusion
    ydb/library/formats/arrow/switch
    ydb/library/actors/core
)

SRCS(
    serialization.cpp
)

END()
