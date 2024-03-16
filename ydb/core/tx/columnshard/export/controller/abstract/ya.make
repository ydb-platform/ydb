LIBRARY()

SRCS(
    abstract.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/tx/datashard
    ydb/library/actors/core
)

END()
