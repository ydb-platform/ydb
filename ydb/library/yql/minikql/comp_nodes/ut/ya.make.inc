FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

REQUIREMENTS(ram:32)

OWNER(g:yql)

INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/minikql/computation/header.ya.make.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/minikql/invoke_builtins/header.ya.make.inc)

SET(ORIG_SRC_DIR ydb/library/yql/minikql/comp_nodes/ut)

SET(ORIG_SOURCES
    mkql_test_factory.cpp
    mkql_bit_utils_ut.cpp
    mkql_block_compress_ut.cpp
    mkql_block_exists_ut.cpp
    mkql_block_skiptake_ut.cpp
    mkql_block_top_sort_ut.cpp
    mkql_blocks_ut.cpp
    mkql_combine_ut.cpp
    mkql_condense_ut.cpp
    mkql_decimal_ut.cpp
    mkql_chain_map_ut.cpp
    mkql_chopper_ut.cpp
    mkql_filters_ut.cpp
    mkql_flatmap_ut.cpp
    mkql_fromstring_ut.cpp
    mkql_multihopping_saveload_ut.cpp
    mkql_multihopping_ut.cpp
    mkql_multimap_ut.cpp
    mkql_fold_ut.cpp
    mkql_heap_ut.cpp
    mkql_compare_ut.cpp
    mkql_computation_node_ut.cpp
    mkql_group_ut.cpp
    mkql_dict_ut.cpp
    mkql_isa_detection_ut.cpp
    mkql_join_ut.cpp
    mkql_join_dict_ut.cpp
    mkql_grace_join_ut.cpp
    mkql_map_join_ut.cpp
    mkql_match_recognize_matched_vars_ut.cpp
    mkql_match_recognize_list_ut.cpp
    mkql_match_recognize_nfa_ut.cpp
    mkql_match_recognize_ut.cpp
    mkql_safe_circular_buffer_ut.cpp
    mkql_sort_ut.cpp
    mkql_switch_ut.cpp
    mkql_time_order_recover_saveload_ut.cpp
    mkql_todict_ut.cpp
    mkql_variant_ut.cpp
    mkql_wide_chain_map_ut.cpp
    mkql_wide_chopper_ut.cpp
    mkql_wide_combine_ut.cpp
    mkql_wide_condense_ut.cpp
    mkql_wide_filter_ut.cpp
    mkql_wide_map_ut.cpp
    mkql_wide_nodes_ut.cpp
    mkql_wide_stream_ut.cpp
    mkql_wide_top_sort_ut.cpp
    mkql_listfromrange_ut.cpp
    mkql_mapnext_ut.cpp
    mkql_rh_hash_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/arrow
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

IF (ARCH_X86_64)

CFLAGS(
    -mprfchw
)

ENDIF()

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

ADDINCL(
    ${ORIG_SRC_DIR}
)

COPY(
    WITH_CONTEXT
    AUTO
    FROM ${ORIG_SRC_DIR}
    ${ORIG_SOURCES}
    OUTPUT_INCLUDES
        ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h
)

