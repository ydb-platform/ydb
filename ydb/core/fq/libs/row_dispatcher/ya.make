LIBRARY()

SRCS(
    actors_factory.cpp
    common.cpp
    coordinator.cpp
    json_filter.cpp
    json_parser.cpp
    leader_election.cpp
    row_dispatcher_service.cpp
    row_dispatcher.cpp
    topic_session.cpp
)

PEERDIR(
    contrib/libs/fmt
    contrib/libs/simdjson
    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/control_plane_storage
    ydb/core/fq/libs/row_dispatcher/events
    ydb/core/fq/libs/row_dispatcher/purecalc_compilation
    ydb/core/fq/libs/shared_resources
    ydb/core/fq/libs/ydb
    ydb/core/mon
    ydb/library/actors/core
    ydb/library/security
    ydb/library/yql/dq/actors/common
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/proto
    ydb/library/yql/providers/pq/provider
    ydb/library/yql/public/purecalc/common/no_pg_wrapper
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
)

CFLAGS(
    -Wno-assume
)

YQL_LAST_ABI_VERSION()

END()

IF(NOT EXPORT_CMAKE)
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
