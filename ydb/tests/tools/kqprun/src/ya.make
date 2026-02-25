LIBRARY()

SRCS(
    actors.cpp
    kqp_runner.cpp
    ydb_setup.cpp
)

PEERDIR(
    library/cpp/protobuf/json
    ydb/core/client/server
    ydb/core/grpc_services
    ydb/core/kqp/workload_service/actors
    ydb/core/testlib
    ydb/library/aclib
    ydb/library/yql/providers/pq/gateway/abstract
    ydb/services/persqueue_v1
    ydb/tests/tools/kqprun/runlib
    ydb/tests/tools/kqprun/src/proto
    yt/yql/providers/yt/mkql_dq
)

GENERATE_ENUM_SERIALIZATION(common.h)

YQL_LAST_ABI_VERSION()

END()
