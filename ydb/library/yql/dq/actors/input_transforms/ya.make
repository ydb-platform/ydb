LIBRARY()

SRCS(
    dq_input_transform_lookup.cpp
    dq_input_transform_lookup_factory.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/yql/minikql
    ydb/library/yql/dq/actors/compute
)

YQL_LAST_ABI_VERSION()

END()
