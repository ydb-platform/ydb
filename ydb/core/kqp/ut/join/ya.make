UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(200)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(cpu:4)
ELSEIF(SANITIZER_TYPE == "memory")
    SIZE(MEDIUM)
    # Keep explicit MSan memory headroom from YQL-19940.
    REQUIREMENTS(ram:16 cpu:4)
ELSEIF(SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:4)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    kqp_block_hash_join_ut.cpp
    kqp_flip_join_ut.cpp
    kqp_index_lookup_join_ut.cpp
    kqp_join_ut.cpp
    kqp_join_order_ut.cpp
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
