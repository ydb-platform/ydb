IF (NOT WITH_VALGRIND)
    UNITTEST_FOR(ydb/core/tx/schemeshard)

    FORK_SUBTESTS()

    SPLIT_FACTOR(60)

    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

    PEERDIR(
        library/cpp/getopt
        library/cpp/regex/pcre
        library/cpp/svnversion
        ydb/core/testlib/default
        ydb/core/tx
        ydb/core/tx/schemeshard/ut_helpers
        yql/essentials/public/udf/service/exception_policy
        ydb/library/dbgtrace
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        ut_pq_reboots.cpp
    )

    END()
ENDIF()
