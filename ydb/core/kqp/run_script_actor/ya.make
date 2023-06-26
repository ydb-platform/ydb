LIBRARY()

SRCS(
    kqp_run_script_actor.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/protobuf/json
    ydb/core/base
    ydb/core/protos
    ydb/core/kqp/common/events
    ydb/core/kqp/executer_actor
    ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
