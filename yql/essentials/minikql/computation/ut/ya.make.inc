FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCDIR(yql/essentials/minikql/computation)

SRCS(
    mkql_block_trimmer_ut.cpp
    mkql_computation_node_holders_ut.cpp
    mkql_computation_node_pack_ut.cpp
    mkql_computation_node_list_ut.cpp
    mkql_computation_node_dict_ut.cpp
    mkql_computation_node_graph_saveload_ut.cpp
    mkql_computation_pattern_cache_ut.cpp
    mkql_key_payload_value_lru_cache_ut.cpp
    mkql_validate_ut.cpp
    mkql_value_builder_ut.cpp
    presort_ut.cpp
    mkql_vector_spiller_adapter_ut.cpp
    mkql_method_address_helper_ut.cpp
    mkql_spiller_adapter_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/threading/local_executor
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()
