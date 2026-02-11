LIBRARY()

SRCS(
    actors_factory.cpp
    coordinator.cpp
    leader_election.cpp
    local_leader_election.cpp
    probes.cpp
    row_dispatcher.cpp
    row_dispatcher_service.cpp
    topic_session.cpp
)

PEERDIR(
    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/metrics
    ydb/core/fq/libs/row_dispatcher/common
    ydb/core/fq/libs/row_dispatcher/events
    ydb/core/fq/libs/row_dispatcher/format_handler
    ydb/core/fq/libs/row_dispatcher/purecalc_compilation
    ydb/core/fq/libs/shared_resources
    ydb/core/fq/libs/ydb

    ydb/core/mon
    ydb/core/mind

    ydb/library/actors/core
    ydb/library/security
    ydb/library/yql/dq/actors
    ydb/library/yql/dq/actors/common
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/proto
    ydb/library/yql/providers/pq/common
    ydb/library/yql/providers/pq/gateway/abstract
    ydb/library/yql/providers/pq/provider

    ydb/public/sdk/cpp/adapters/issue
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    purecalc_no_pg_wrapper
    format_handler
)

IF(NOT EXPORT_CMAKE)
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
