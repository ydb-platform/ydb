UNITTEST_FOR(ydb/core/client)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    REQUIREMENTS(
        cpu:4
        ram:32
    )
    TAG(ya:fat)
ELSE()
    REQUIREMENTS(
        cpu:4
        ram:16
    )
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/client/scheme_cache_lib
    ydb/core/tablet_flat/test/libs/rows
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/supp/ubsan_supp.inc)

SRCS(
    cancel_tx_ut.cpp
    client_ut.cpp
    flat_ut.cpp
    locks_ut.cpp
    query_stats_ut.cpp
    s3_listing_ut.cpp
)

END()
