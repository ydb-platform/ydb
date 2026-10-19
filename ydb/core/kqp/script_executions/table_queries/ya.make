LIBRARY()

SRCS(
    kqp_script_executions.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/json
    library/cpp/protobuf/interop
    library/cpp/protobuf/json
    library/cpp/retry
    ydb/core/fq/libs/common
    ydb/core/kqp/common
    ydb/core/kqp/common/events
    ydb/core/kqp/counters
    ydb/core/kqp/script_executions/common
    ydb/core/kqp/script_executions/proto
    ydb/core/kqp/script_executions/run_script_actor
    ydb/core/protos
    ydb/core/util
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/query_actor
    ydb/library/table_creator
    ydb/library/yql/providers/pq/proto
    ydb/public/api/protos
    ydb/public/lib/scheme_types
    ydb/public/sdk/cpp/src/client/params
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/library/operation_id
    yql/essentials/public/issue
)

GENERATE_ENUM_SERIALIZATION(kqp_script_executions_impl.h)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
