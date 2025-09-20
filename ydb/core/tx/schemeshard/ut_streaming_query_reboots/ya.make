IF (NOT WITH_VALGRIND)
    UNITTEST_FOR(ydb/core/tx/schemeshard)

    FORK_SUBTESTS()

    SPLIT_FACTOR(60)

    IF (SANITIZER_TYPE)
        SIZE(LARGE)
        TAG(ya:fat)
        REQUIREMENTS(ram:12)
    ELSE()
        SIZE(MEDIUM)
    ENDIF()

    PEERDIR(
        ydb/core/testlib/default
        ydb/core/tx/schemeshard/ut_helpers
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        ut_streaming_query_reboots.cpp
    )

    END()
ENDIF()
