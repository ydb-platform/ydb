UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(80)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    ydb/core/base
    ydb/core/scheme
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
    ydb/public/lib/deprecated/kicli
)

SRCS(
    ut_async_index.cpp
    ut_unique_index.cpp
    ut_vector_index.cpp
)

YQL_LAST_ABI_VERSION()

END()
