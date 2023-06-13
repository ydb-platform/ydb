LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/base
    ydb/core/keyvalue
    ydb/core/protos
    ydb/core/tablet
    ydb/public/api/protos
    ydb/library/persqueue/topic_parser
)

END()
