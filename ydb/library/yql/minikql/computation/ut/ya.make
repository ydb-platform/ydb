UNITTEST_FOR(ydb/library/yql/minikql/computation)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

OWNER(
    vvvv
    g:kikimr
)

SRCS(
    mkql_computation_node_pack_ut.cpp
    mkql_computation_node_list_ut.cpp 
    mkql_computation_node_dict_ut.cpp
    mkql_computation_node_graph_saveload_ut.cpp
    mkql_validate_ut.cpp
    mkql_value_builder_ut.cpp 
    presort_ut.cpp
)

PEERDIR(
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
