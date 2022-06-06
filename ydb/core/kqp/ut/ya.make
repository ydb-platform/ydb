UNITTEST_FOR(ydb/core/kqp)

OWNER(
    spuchin
    g:kikimr
)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    TIMEOUT(2400)
    TAG(ya:fat)
    SIZE(LARGE)
    SPLIT_FACTOR(40)
ELSE()
    SPLIT_FACTOR(40)
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_acl_ut.cpp
    kqp_arrow_in_channels_ut.cpp
    kqp_document_api_ut.cpp
    kqp_explain_ut.cpp
    kqp_flip_join_ut.cpp
    kqp_index_lookup_join_ut.cpp
    kqp_indexes_ut.cpp
    kqp_indexes_multishard_ut.cpp
    kqp_join_ut.cpp
    kqp_limits_ut.cpp
    kqp_locks_ut.cpp
    kqp_merge_connection_ut.cpp
    kqp_mvcc_ut.cpp
    kqp_ne_inplace_update_ut.cpp
    kqp_ne_effects_ut.cpp
 #   kqp_ne_flowcontrol_ut.cpp
    kqp_ne_perf_ut.cpp
    kqp_ne_ut.cpp
    kqp_not_null_columns_ut.cpp
    kqp_olap_ut.cpp
    kqp_params_ut.cpp
    kqp_pragma_ut.cpp
    kqp_query_ut.cpp
    kqp_scan_spilling_ut.cpp
    kqp_scan_ut.cpp
    kqp_scheme_ut.cpp
    kqp_scripting_ut.cpp
    kqp_service_ut.cpp
    kqp_sort_ut.cpp
    kqp_stats_ut.cpp
    kqp_sqlin_ut.cpp
    kqp_sys_view_ut.cpp
    kqp_sys_col_ut.cpp
    kqp_table_predicate_ut.cpp
    kqp_tx_ut.cpp
    kqp_types_arrow_ut.cpp
    kqp_write_ut.cpp
    kqp_yql_ut.cpp
)

PEERDIR(
    library/cpp/threading/local_executor
    ydb/core/client/minikql_compile
    ydb/core/kqp
    ydb/core/kqp/counters
    ydb/core/kqp/host
    ydb/core/kqp/provider
    ydb/core/kqp/ut/common
    ydb/public/sdk/cpp/client/draft
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/library/yql/udfs/common/re2
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:12)

END()

RECURSE(
    fat
    ../rm/ut
    ../proxy/ut
    ../runtime/ut
)
