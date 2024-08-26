LIBRARY()

SRCS(
    actors_factory.cpp
    row_dispatcher.cpp
    row_dispatcher_service.cpp
    coordinator.cpp
    leader_election.cpp
    topic_session.cpp
    json_filter.cpp
    json_parser.cpp
)

PEERDIR(
    contrib/libs/fmt
    ydb/library/actors/core
    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/control_plane_storage
    ydb/core/fq/libs/ydb
    ydb/core/fq/libs/row_dispatcher/events
    ydb/core/fq/libs/shared_resources
    ydb/library/security
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/proto
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
    ydb/library/yql/providers/pq/provider
    ydb/library/yql/public/purecalc/common/no_pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
)

RECURSE_FOR_TESTS(
    ut
)
