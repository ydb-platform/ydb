UNITTEST_FOR(ydb/core/tx/columnshard)

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
    ydb/core/tx/columnshard/test_helper
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/hooks/testing
    ydb/services/metadata
    ydb/core/tx
    ydb/public/lib/yson_value
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_columnshard_read_write.cpp
    ut_normalizer.cpp
    ut_backup.cpp
)

END()
