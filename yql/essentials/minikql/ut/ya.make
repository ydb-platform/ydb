UNITTEST_FOR(yql/essentials/minikql)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    aligned_page_pool_ut.cpp
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
    mkql_unboxed_value_stream_ut.cpp
    pack_num_ut.cpp
    watermark_tracker_ut.cpp
)

ADDINCL(
    yql/essentials/parser/pg_wrapper/postgresql/src/include
)

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
