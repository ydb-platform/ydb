LIBRARY()

SRCS(
    memory.cpp
)

PEERDIR(
    contrib/libs/apache/arrow_next
    ydb/core/protos
    ydb/core/formats/arrow
)

END()
