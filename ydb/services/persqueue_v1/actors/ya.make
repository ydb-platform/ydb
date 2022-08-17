LIBRARY()

OWNER(
    g:kikimr
    g:logbroker
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/containers/disjoint_interval_tree
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/persqueue
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tx/scheme_cache
    ydb/library/aclib
    ydb/library/persqueue/topic_parser
    ydb/public/api/protos
    ydb/public/lib/base
    ydb/services/lib/actors
)

SRCS(
    codecs.cpp
    persqueue_utils.cpp
    helpers.cpp
    partition_actor.cpp
    read_init_auth_actor.cpp
    read_info_actor.cpp
    write_session_actor.cpp
    schema_actors.cpp
)

END()
