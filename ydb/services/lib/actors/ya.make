LIBRARY()

OWNER(g:kikimr)

SRCS(
    pq_schema_actor.cpp
)

PEERDIR(
    library/cpp/grpc/server
    library/cpp/digest/md5
    ydb/core/grpc_services
    ydb/core/mind
    ydb/library/persqueue/obfuscate
    ydb/library/persqueue/topic_parser
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/lib/jwt
    ydb/public/lib/operation_id
)

END()
