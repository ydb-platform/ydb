UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(200)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

IF(SANITIZER_TYPE == "memory")
    # Increase MSan memory limit due to YQL-19940.
    # Just double default memory requirements since we run MSan without origin tracking by default.
    REQUIREMENTS(
        ram:16
    )
ENDIF()

SRCS(
    kqp_block_hash_join_ut.cpp
    kqp_join_order_ut.cpp
    kqp_join_topology_generator.cpp
    kqp_join_topology_ut.cpp
    kqp_benches_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
    yql/essentials/udfs/common/digest
)


DATA (
    arcadia/ydb/core/kqp/ut/join
    arcadia/ydb/library/benchmarks/queries
    arcadia/ydb/library/benchmarks/gen_queries/consts.yql
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    index_lookup
)
