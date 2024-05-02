FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

OWNER(g:yql)

SRCDIR(ydb/library/yql/minikql/computation)

SRCS(
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
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/threading/local_executor
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/dq/proto
)

YQL_LAST_ABI_VERSION()
