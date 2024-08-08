UNITTEST_FOR(ydb/core/keyvalue)

FORK_SUBTESTS()

SPLIT_FACTOR(30)
IF (WITH_VALGRIND OR SANITIZER_TYPE  == "thread")
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
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
