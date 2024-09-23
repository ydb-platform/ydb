LIBRARY()

SRCS(
    mkql_block_builder.cpp
    mkql_block_impl.cpp
    mkql_block_reader.cpp
    mkql_block_transport.cpp
    mkql_computation_node.cpp
    mkql_computation_node_holders.cpp
    mkql_computation_node_impl.cpp
    mkql_computation_node_pack.cpp
    mkql_computation_node_pack_impl.cpp
    mkql_custom_list.cpp
    mkql_validate.cpp
    mkql_value_builder.cpp
    mkql_computation_pattern_cache.cpp
    presort.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/yql/public/types
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/library/yql/public/udf
    ydb/library/yql/minikql/arrow
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    llvm14
    no_llvm
)

RECURSE_FOR_TESTS(
    llvm14/ut
)
