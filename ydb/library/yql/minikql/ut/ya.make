UNITTEST_FOR(ydb/library/yql/minikql)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    compact_hash_ut.cpp
    mkql_alloc_ut.cpp
    mkql_node_builder_ut.cpp
    mkql_node_cast_ut.cpp
    mkql_node_printer_ut.cpp
    mkql_node_ut.cpp
    mkql_opt_literal_ut.cpp
    mkql_stats_registry_ut.cpp
    mkql_string_util_ut.cpp
    mkql_type_builder_ut.cpp
    mkql_type_ops_ut.cpp
    pack_num_ut.cpp
    watermark_tracker_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/yql/minikql/computation
    ydb/library/yql/minikql/invoke_builtins
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
