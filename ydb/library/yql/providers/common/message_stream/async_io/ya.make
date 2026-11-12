YQL_LIBRARY()
SRCS(read_actor.cpp)
PEERDIR(
    library/cpp/containers/disjoint_interval_tree
    ydb/library/actors/core
    ydb/library/services
    ydb/library/yql/providers/abstract
    ydb/library/yql/providers/common/message_stream
    ydb/library/yql/dq/actors/compute
    yql/essentials/minikql/computation
    yql/essentials/utils/log
)
END()

RECURSE_FOR_TESTS(ut)
