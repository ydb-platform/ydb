UNITTEST_FOR(ydb/core/tx/columnshard)

OWNER(
    chertus
    g:kikimr
)

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
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib
    ydb/core/tx
    ydb/public/lib/yson_value
)

YQL_LAST_ABI_VERSION()

SRCS(
    columnshard_ut_common.cpp
    ut_columnshard_read_write.cpp
    ut_columnshard_schema.cpp
)

END()
