LIBRARY()

SRCS(
    mkql_block_builder.cpp
    mkql_block_impl.cpp
    mkql_block_reader.cpp
    mkql_block_transport.cpp
    mkql_block_trimmer.cpp
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
    yql/essentials/public/types
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/minikql/arrow
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    llvm16
    no_llvm
)

RECURSE_FOR_TESTS(
    llvm16/ut
)
