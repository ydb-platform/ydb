LIBRARY()

SRCS(
    mkql_block_builder.cpp
    mkql_block_impl.cpp
    mkql_block_reader.cpp
    mkql_block_transport.cpp
    mkql_block_trimmer.cpp
    mkql_computation_node.cpp
    mkql_datum_validate.cpp
    mkql_computation_node_holders.cpp
    mkql_computation_node_impl.cpp
    mkql_computation_node_pack.cpp
    mkql_computation_node_pack_impl.cpp
    mkql_custom_list.cpp
    mkql_validate.cpp
    mkql_value_builder.cpp
    mkql_computation_pattern_cache.cpp
    presort.cpp
    mkql_spiller_adapter_ut.cpp
    mkql_vector_spiller_adapter_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/public/types
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/minikql/arrow
    yql/essentials/minikql/computation
    yql/essentials/minikql/mkql_type_builder
    yql/essentials/minikql/mkql_node
    yql/essentials/minikql/mkql_alloc
    library/cpp/testing/unittest
    library/cpp/threading/future/core
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

TEST_TYPE = UNIT
