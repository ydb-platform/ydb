LIBRARY()

SRCS(
    result.cpp
    status.cpp
    ydb.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/library/actors/core
    ydb/library/conclusion/generic
)

END()
