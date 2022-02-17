UNITTEST_FOR(ydb/library/yql/minikql/comp_nodes)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
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
    mkql_blocks_ut.cpp
    mkql_combine_ut.cpp
    mkql_condense_ut.cpp
    mkql_decimal_ut.cpp
    mkql_chain_map_ut.cpp
    mkql_chopper_ut.cpp
    mkql_filters_ut.cpp
    mkql_flatmap_ut.cpp
    mkql_multihopping_saveload_ut.cpp
    mkql_multihopping_ut.cpp
    mkql_multimap_ut.cpp
    mkql_fold_ut.cpp
    mkql_heap_ut.cpp
    mkql_compare_ut.cpp
    mkql_computation_node_ut.cpp
    mkql_group_ut.cpp
    mkql_dict_ut.cpp
    mkql_join_ut.cpp
    mkql_join_dict_ut.cpp
    mkql_map_join_ut.cpp
    mkql_safe_circular_buffer_ut.cpp
    mkql_sort_ut.cpp
    mkql_switch_ut.cpp
    mkql_todict_ut.cpp
    mkql_variant_ut.cpp
    mkql_wide_chain_map_ut.cpp
    mkql_wide_chopper_ut.cpp
    mkql_wide_combine_ut.cpp
    mkql_wide_condense_ut.cpp
    mkql_wide_filter_ut.cpp
    mkql_wide_map_ut.cpp
    mkql_wide_nodes_ut.cpp
    mkql_listfromrange_ut.cpp
    mkql_mapnext_ut.cpp
)

PEERDIR(
    ydb/library/yql/minikql/computation
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

END()
