LIBRARY()

PEERDIR(
    ydb/library/actors/core
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/runtime
    ydb/library/yql/minikql/comp_nodes/packed_tuple
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/computation
    yql/essentials/utils
)

SRCS(
    yql_common_dq_factory.cpp
    dq_hash_aggregate.cpp
    dq_hash_combine.cpp
    dq_hash_operator_common.cpp
    dq_hash_operator_serdes.cpp
    dq_program_builder.cpp
    dq_block_hash_join.cpp
    mkql_resource_meter.cpp
    block_layout_converter.cpp
)


YQL_LAST_ABI_VERSION()


END()

RECURSE_FOR_TESTS(
    ut
)
