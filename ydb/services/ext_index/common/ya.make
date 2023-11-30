LIBRARY()

SRCS(
    service.cpp
    config.cpp
    events.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/public/api/protos
    ydb/core/protos
    contrib/libs/apache/arrow
)

END()
