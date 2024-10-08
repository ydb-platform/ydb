IF (NOT WITH_VALGRIND)
    UNITTEST_FOR(ydb/core/tx/schemeshard)

    FORK_SUBTESTS()

    SPLIT_FACTOR(60)

    IF (SANITIZER_TYPE OR WITH_VALGRIND)
        TIMEOUT(3600)
        SIZE(LARGE)
        TAG(ya:fat)
        REQUIREMENTS(ram:12)
    ELSE()
        TIMEOUT(600)
        SIZE(MEDIUM)
    ENDIF()

    PEERDIR(
        library/cpp/getopt
        library/cpp/regex/pcre
        library/cpp/svnversion
        ydb/core/testlib/default
        ydb/core/tx
        ydb/core/tx/schemeshard/ut_helpers
        ydb/library/yql/public/udf/service/exception_policy
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        ut_external_data_source_reboots.cpp
    )

END()
ENDIF()
