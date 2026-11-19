YQL_LIBRARY()

SRCS(
    query_plan.cpp
    rbo_plan.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/protobuf/json
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/yql/dq/actors/protos
    yql/essentials/utils/log
)

END()

RECURSE_FOR_TESTS(
    ut
)
