UNITTEST_FOR(ydb/services/ext_index)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
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
    ydb/services/metadata
    ydb/services/ext_index
    ydb/public/lib/yson_value
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_ext_index.cpp
)

END()
