UNITTEST_FOR(ydb/core/keyvalue)

FORK_SUBTESTS()

IF (WITH_VALGRIND OR SANITIZER_TYPE)
    TIMEOUT(1800)
    SIZE(LARGE)
    SPLIT_FACTOR(20)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
    SPLIT_FACTOR(10)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    keyvalue_ut.cpp
    keyvalue_collector_ut.cpp
    keyvalue_storage_read_request_ut.cpp
)

END()
