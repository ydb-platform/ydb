LIBRARY()

SRCS(
    dq_input_transform_lookup.cpp
    dq_input_transform_lookup_factory.cpp
)

PEERDIR(
    ydb/library/actors/core
    yql/essentials/minikql
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/runtime/streaming
)

YQL_LAST_ABI_VERSION()

END()
